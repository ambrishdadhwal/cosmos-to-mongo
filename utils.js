const constants = require("./constants");
const moment = require('moment');
// const BulkMigrationCheckPoint = moment.utc([2024, '09', 28, 10, '00', '00', '000']).valueOf(); // 28th October 2024
const BulkMigrationCheckPoint = moment.utc([2024, '10', 17, 01, '00', '00', '000']).valueOf();
const QueryMap = require('./queryMap.json');
const _ = require('lodash');

const isLocalHost = (host) => {
    const exists = constants.LOCALHOSTS.find((item) => host.indexOf(item) !== -1);
    return !!exists;
};

const getConnectionString = (dbConfig) => {
    const {
        username, password, host, databaseName,
    } = dbConfig;

    if (!host) {
        throw new Error('MongoDB Host is missing');
    }

    if (!databaseName) {
        throw new Error('MongoDB Database Name is missing');
    }

    let connectionString = '';

    if (isLocalHost(host)) {
        connectionString = `mongodb://${host}/${databaseName}`;
    } else {
        if (!username || !password) {
            throw new Error('MongoDB Credentials is missing');
        }
		
		//const encodedPass = _.escape(password);

        connectionString = `mongodb+srv://${username}:${password}@${host}/${databaseName}?retryWrites=true&w=majority`;
    }
	console.log('connectionString ----', connectionString);

    return connectionString;
};

const getFiltersByCollectionName = (collectionName, item) => {
    if (collectionName === 'addressAreas') {
        return { country: item.country };
    } else {
         //return { _id: item._id, updatedAt: { $lte: item.updatedAt } }
         return { offerId: item.offerId }
    }
}

const getQueryByContainerId = (containerId) => {
    let query = QueryMap[containerId];
	const dateString = "1970-01-01T10:00:00";
	const milliseconds = moment(dateString).valueOf();
    switch (containerId) {
        case 'addressAreas': {
            break;
        }
        default:
            query = `${query} where c._ts >= ${milliseconds}`;
            break;
    }
    return query;
}

const formatItem = (item) => {
    const { deletedAt, updatedAt, expiredAt, createdAt, ...rest } = item
    const obj = {
        ...rest,
    };
    if (createdAt) {
        obj.createdAt = new Date(createdAt);
    }
    if (updatedAt) {
        obj.updatedAt = new Date(updatedAt);
    }
    if (deletedAt) {
        obj.deletedAt = new Date(deletedAt);
    }
    if (expiredAt) {
        obj.expiredAt = new Date(expiredAt);
    }
    return obj;
}

const formatCustomer = (customer) => {
    if (customer.createdAt) {
        customer.createdAt = new Date(customer.createdAt);
    }
    if (customer.updatedAt) {
        customer.updatedAt = new Date(customer.updatedAt);

    }
    if (customer.deletedAt) {
        customer.deletedAt = new Date(customer.deletedAt);
    }
    if (customer.lastLoginAt) {
        customer.lastLoginAt = new Date(customer.lastLoginAt);
    }
    //contacts
    if (customer.contacts) {
        customer.contacts = customer?.contacts?.map(contact => {
            if (contact.createdAt) {
                contact.createdAt = new Date(contact.createdAt);
            }
            if (contact.updatedAt) {
                contact.updatedAt = new Date(contact.updatedAt);
            }
            if (contact.deletedAt) {
                contact.deletedAt = new Date(contact.deletedAt);
            }
            if (contact.verifiedAt) {
                contact.verifiedAt = new Date(contact.verifiedAt);
            }
            return contact;
        });
    }

    if (customer?.profile?.dateOfBirth) {
        customer.profile.dateOfBirth = new Date(customer.profile.dateOfBirth);
    }

    if (customer.policyAcceptance) {
        customer.policyAcceptance = customer.policyAcceptance?.map((item) => {
            if (item.createdAt) {
                item.createdAt = new Date(item.createdAt);
            }
            return item;
        });
    }

    if (customer.migrationStatus) {
        if (customer.migrationStatus.auth0?.migratedAt) {
            customer.migrationStatus.auth0.migratedAt = new Date(customer.migrationStatus.auth0.migratedAt);
        }
        if (customer.migrationStatus.CDBOauth?.migratedAt) {
            customer.migrationStatus.CDBOauth.migratedAt = new Date(customer.migrationStatus.CDBOauth.migratedAt);
        }
        if (customer.migrationStatus.share?.AE?.migratedAt) {
            customer.migrationStatus.share.AE.migratedAt = new Date(customer.migrationStatus.share.AE.migratedAt);
        }
        if (customer.migrationStatus.share?.AE?.pointsMigratedAt) {
            customer.migrationStatus.share.AE.pointsMigratedAt = new Date(customer.migrationStatus.share.AE.pointsMigratedAt);
        }
        if (customer.migrationStatus.myclub?.BH?.migratedAt) {
            customer.migrationStatus.myclub.BH.migratedAt = new Date(customer.migrationStatus.myclub.BH.migratedAt);
        }
        if (customer.migrationStatus.myclub?.BH?.pointsMigratedAt) {
            customer.migrationStatus.myclub.BH.pointsMigratedAt = new Date(customer.migrationStatus.myclub.BH.pointsMigratedAt);
        }
        if (customer.migrationStatus.myclub?.KW?.migratedAt) {
            customer.migrationStatus.myclub.KW.migratedAt = new Date(customer.migrationStatus.myclub.KW.migratedAt);
        }
        if (customer.migrationStatus.myclub?.KW?.pointsMigratedAt) {
            customer.migrationStatus.myclub.KW.pointsMigratedAt = new Date(customer.migrationStatus.myclub.KW.pointsMigratedAt);
        }
    }
    return customer;
}

const deleteCosmosSpecificFields = (obj) => {
    delete obj._rid;
    delete obj._self;
    delete obj._etag;
    delete obj._attachments;
    return obj;
}

const mapper = (collection, item) => {
    let { id, ...obj } = item;
    deleteCosmosSpecificFields(obj);
    switch (collection) {
        case 'addressAreas': return obj;
        case 'customers': return formatCustomer(obj);
        default: return formatItem(obj);
    }
}

module.exports = {
    isLocalHost,
    getConnectionString,
    getQueryByContainerId,
    getFiltersByCollectionName,
    mapper
};