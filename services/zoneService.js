const { Zone, Warehouse } = require('../models');
const { Op } = require('sequelize');

async function list(reqUser, query = {}) {
  const where = {};
  if (query.warehouseId) where.warehouseId = query.warehouseId;
  if (reqUser.role === 'company_admin' || reqUser.role === 'inventory_manager') {
    if (reqUser.companyId) where.companyId = reqUser.companyId;
  } else if (reqUser.role !== 'super_admin' && reqUser.warehouseId) {
    where.warehouseId = reqUser.warehouseId;
  }
  const zones = await Zone.findAll({
    where,
    order: [['createdAt', 'DESC']],
    include: [{ association: 'Warehouse', attributes: ['id', 'name', 'code'] }],
  });
  return zones;
}

async function getById(id, reqUser) {
  const zone = await Zone.findByPk(id, {
    include: [{ association: 'Warehouse' }, { association: 'Locations' }],
  });
  if (!zone) throw new Error('Zone not found');
  return zone;
}

async function create(data, reqUser) {
  if (!data.warehouseId) throw new Error('warehouseId required');
  let companyId = reqUser.companyId;
  if (!companyId && data.warehouseId) {
    const wh = await Warehouse.findByPk(data.warehouseId);
    if (wh) companyId = wh.companyId;
  }

  return Zone.create({
    companyId,
    warehouseId: data.warehouseId,
    name: data.name,
    code: data.code || null,
    zoneType: data.zoneType || null,
  });
}

async function update(id, data, reqUser) {
  const zone = await Zone.findByPk(id);
  if (!zone) throw new Error('Zone not found');
  await zone.update({
    name: data.name ?? zone.name,
    code: data.code !== undefined ? data.code : zone.code,
    zoneType: data.zoneType !== undefined ? data.zoneType : zone.zoneType,
  });
  return zone;
}

async function remove(id, reqUser) {
  const zone = await Zone.findByPk(id);
  if (!zone) throw new Error('Zone not found');
  await zone.destroy();
  return { message: 'Zone deleted' };
}

module.exports = { list, getById, create, update, remove };
