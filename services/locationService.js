const { Location, Zone, Warehouse } = require('../models');
const { Op } = require('sequelize');

function normalizeRole(role) {
  return (role || '').toString().toLowerCase().replace(/-/g, '_').trim();
}

/**
 * Formats location name according to Aisle + Rack + Shelf + Bin without dashes
 */
function formatLocationName(data) {
  const parts = [data.aisle, data.rack, data.shelf, data.bin];
  const formatted = parts
    .filter(p => p != null && p !== '')
    .map(p => p.toString().replace(/-/g, ''))
    .join('');
  
  if (formatted) return formatted;
  return data.name ? data.name.replace(/-/g, '') : null;
}

async function list(reqUser, query = {}) {
  const where = {};
  if (query.zoneId) where.zoneId = query.zoneId;
  if (query.warehouseId) {
    const zoneIds = await Zone.findAll({ where: { warehouseId: query.warehouseId }, attributes: ['id'] });
    where.zoneId = { [Op.in]: zoneIds.map(z => z.id) };
  }
  const role = normalizeRole(reqUser.role);
  // super_admin: no company/warehouse filter -> show all locations
  if (role !== 'super_admin') {
    if (role === 'company_admin' && reqUser.companyId) {
      const whIds = await Warehouse.findAll({ where: { companyId: reqUser.companyId }, attributes: ['id'] });
      const whIdList = whIds.map(w => w.id);
      if (whIdList.length > 0) {
        const zoneRows = await Zone.findAll({ where: { warehouseId: { [Op.in]: whIdList } }, attributes: ['id'] });
        const zoneIdList = zoneRows.map(z => z.id);
        where.zoneId = zoneIdList.length > 0 ? { [Op.in]: zoneIdList } : { [Op.in]: [] };
      } else {
        where.zoneId = { [Op.in]: [] };
      }
    } else if (reqUser.warehouseId) {
      const zoneIds = await Zone.findAll({ where: { warehouseId: reqUser.warehouseId }, attributes: ['id'] });
      where.zoneId = { [Op.in]: zoneIds.map(z => z.id) };
    }
  }
  const locations = await Location.findAll({
    where,
    order: [['createdAt', 'DESC']],
    include: [{ association: 'Zone', include: [{ association: 'Warehouse', attributes: ['id', 'name', 'code'] }] }],
  });
  return locations.map(loc => (loc.get ? loc.get({ plain: true }) : loc));
}

async function getById(id, reqUser) {
  const loc = await Location.findByPk(id, {
    include: [{ association: 'Zone', include: ['Warehouse'] }],
  });
  if (!loc) throw new Error('Location not found');
  return loc;
}

async function create(data, reqUser) {
  if (!data.zoneId) throw new Error('zoneId required');
  
  const formattedName = formatLocationName(data);
  
  return Location.create({
    zoneId: data.zoneId,
    name: formattedName || data.name,
    code: data.code || null,
    aisle: data.aisle || null,
    rack: data.rack || null,
    shelf: data.shelf || null,
    bin: data.bin || null,
    locationType: data.locationType || null,
    pickSequence: data.pickSequence != null ? Number(data.pickSequence) : null,
    maxWeight: data.maxWeight != null ? Number(data.maxWeight) : null,
    heatSensitive: data.heatSensitive || null,
  });
}

async function update(id, data, reqUser) {
  const loc = await Location.findByPk(id);
  if (!loc) throw new Error('Location not found');

  const formattedName = formatLocationName(data);

  await loc.update({
    name: formattedName || data.name || loc.name,
    code: data.code !== undefined ? data.code : loc.code,
    aisle: data.aisle !== undefined ? data.aisle : loc.aisle,
    rack: data.rack !== undefined ? data.rack : loc.rack,
    shelf: data.shelf !== undefined ? data.shelf : loc.shelf,
    bin: data.bin !== undefined ? data.bin : loc.bin,
    locationType: data.locationType !== undefined ? data.locationType : loc.locationType,
    pickSequence: data.pickSequence !== undefined ? (data.pickSequence != null ? Number(data.pickSequence) : null) : loc.pickSequence,
    maxWeight: data.maxWeight !== undefined ? (data.maxWeight != null ? Number(data.maxWeight) : null) : loc.maxWeight,
    heatSensitive: data.heatSensitive !== undefined ? data.heatSensitive : loc.heatSensitive,
  });
  return loc;
}

async function remove(id, reqUser) {
  const loc = await Location.findByPk(id);
  if (!loc) throw new Error('Location not found');
  await loc.destroy();
  return { message: 'Location deleted' };
}

async function bulkCreate(locationsData, reqUser) {
  const results = [];
  const errors = [];
  const namesInBatch = new Set(); 

  let successCount = 0;
  let failureCount = 0;

  const getValue = (row, keys = []) => {
    for (const key of keys) {
      if (row[key] !== undefined && row[key] !== null && String(row[key]).trim() !== '') {
        return String(row[key]).trim();
      }
    }
    return null;
  };

  const normalizeLocationType = (value) => {
    if (!value) return 'PICK';
    const t = String(value).trim().toUpperCase();
    return ['PICK', 'BULK', 'QUARANTINE', 'STAGING'].includes(t) ? t : 'PICK';
  };
  
  try {
    // Cache zones for the company to resolve names to IDs
    let zoneWhere = {};
    if (reqUser.role !== 'super_admin') {
        const whWhere = {};
        if (reqUser.companyId) whWhere.companyId = reqUser.companyId;
        else if (reqUser.warehouseId) whWhere.id = reqUser.warehouseId;
        
        const warehouses = await Warehouse.findAll({ where: whWhere, attributes: ['id'] });
        const whIds = warehouses.map(w => w.id);
        zoneWhere = { warehouseId: { [Op.in]: whIds } };
    }
    
    const existingZones = await Zone.findAll({ 
      where: zoneWhere,
      attributes: ['id', 'name', 'code', 'warehouseId']
    });
    const zoneMap = new Map();
    const validZoneIds = new Set();
    existingZones.forEach(z => {
      zoneMap.set(z.name.toLowerCase().trim(), z.id);
      if (z.code) zoneMap.set(z.code.toLowerCase().trim(), z.id);
      validZoneIds.add(z.id);
    });

    for (const [index, item] of locationsData.entries()) {
      try {
        const zoneInput = getValue(item, ['zoneId', 'zoneid', 'zone_id', 'ZoneId', 'Zone ID', 'ZoneID', '\uFEFFzoneId', 'zone', 'Zone', 'Zone Name', 'ZoneName']);
        let zoneId = null;

        if (zoneInput != null && zoneInput !== '') {
          if (!isNaN(zoneInput)) {
            const numericId = Number(zoneInput);
            if (validZoneIds.has(numericId)) {
              zoneId = numericId;
            } else {
              // Fallback
              const warehousesWithThisId = await Warehouse.findAll({ 
                where: { id: numericId, ...(reqUser.companyId ? { companyId: reqUser.companyId } : {}) },
                include: [{ association: 'Zones' }]
              });
              
              if (warehousesWithThisId.length > 0) {
                const wh = warehousesWithThisId[0];
                if (wh.Zones && wh.Zones.length === 1) {
                  zoneId = wh.Zones[0].id;
                } else if (wh.Zones && wh.Zones.length > 1) {
                  throw new Error(`ID ${numericId} matches a Warehouse with multiple zones. Specify Zone ID.`);
                }
              }
              
              if (!zoneId) {
                throw new Error(`Zone ID ${numericId} not found or unauthorized.`);
              }
            }
          } else {
            const lowerName = zoneInput.toLowerCase().trim();
            if (zoneMap.has(lowerName)) {
              zoneId = zoneMap.get(lowerName);
            } else {
              throw new Error(`Zone name/code "${zoneInput}" not found.`);
            }
          }
        }

        if (!zoneId) throw new Error(`zoneId or zoneName is required`);

        const normalized = {
          zoneId,
          name: getValue(item, ['name', 'Name']),
          code: getValue(item, ['code', 'Code']),
          aisle: getValue(item, ['aisle', 'Aisle']),
          rack: getValue(item, ['rack', 'Rack']),
          shelf: getValue(item, ['shelf', 'Shelf']),
          bin: getValue(item, ['bin', 'Bin']),
          locationType: normalizeLocationType(getValue(item, ['locationType', 'location_type', 'Location Type', 'Type'])),
          pickSequence: getValue(item, ['pickSequence', 'pick_sequence', 'Pick Sequence']),
          maxWeight: getValue(item, ['maxWeight', 'max_weight', 'Max Weight']),
          heatSensitive: getValue(item, ['heatSensitive', 'heat_sensitive', 'Heat Sensitive']),
        };
        
        const locName = formatLocationName(normalized) || normalized.name;
        if (!locName) throw new Error(`Location name could not be generated.`);

        const batchKey = `${zoneId}-${locName}`;
        if (namesInBatch.has(batchKey)) {
          throw new Error(`Duplicate location name "${locName}" in CSV for this zone.`);
        }
        namesInBatch.add(batchKey);

        const existingInDb = await Location.findOne({ where: { name: locName, zoneId } });
        
        if (existingInDb) {
          await existingInDb.update({
            code: normalized.code || existingInDb.code,
            aisle: normalized.aisle || existingInDb.aisle,
            rack: normalized.rack || existingInDb.rack,
            shelf: normalized.shelf || existingInDb.shelf,
            bin: normalized.bin || existingInDb.bin,
            locationType: normalized.locationType,
            pickSequence: normalized.pickSequence != null ? Number(normalized.pickSequence) : existingInDb.pickSequence,
            maxWeight: normalized.maxWeight != null ? Number(normalized.maxWeight) : existingInDb.maxWeight,
            heatSensitive: normalized.heatSensitive || existingInDb.heatSensitive,
          });
          results.push(existingInDb);
        } else {
          const loc = await Location.create({
            zoneId,
            name: locName,
            code: normalized.code || null,
            aisle: normalized.aisle || null,
            rack: normalized.rack || null,
            shelf: normalized.shelf || null,
            bin: normalized.bin || null,
            locationType: normalized.locationType,
            pickSequence: normalized.pickSequence != null ? Number(normalized.pickSequence) : null,
            maxWeight: normalized.maxWeight != null ? Number(normalized.maxWeight) : null,
            heatSensitive: normalized.heatSensitive || null,
          });
          results.push(loc);
        }
        successCount++;
      } catch (err) {
        failureCount++;
        errors.push({
          row: index + 2,
          message: err.message
        });
      }
    }

    return {
      success: true,
      message: `Processed ${locationsData.length} rows. ${successCount} successful, ${failureCount} failed.`,
      successCount: Number(successCount || 0),
      failureCount: Number(failureCount || 0),
      errors: errors || []
    };
  } catch (err) {
    console.error('Bulk Import Service Error:', err);
    throw err;
  }
}


async function migrateExistingLocations() {
    const locations = await Location.findAll();
    for (const loc of locations) {
        const newName = formatLocationName({
            aisle: loc.aisle,
            rack: loc.rack,
            shelf: loc.shelf,
            bin: loc.bin,
            name: loc.name
        });
        if (newName && newName !== loc.name) {
            await loc.update({ name: newName });
        }
    }
}

module.exports = { list, getById, create, update, remove, bulkCreate, migrateExistingLocations };

