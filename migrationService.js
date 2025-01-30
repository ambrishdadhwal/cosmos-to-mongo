require('dotenv').config();
const config = require('./config');
const { CosmosClient } = require('@azure/cosmos');
const { MongoClient, ServerApiVersion } = require('mongodb');
const { mongo: MongoConfig, cosmos: CosmosConfig } = config;
const QueryMap = require('./queryMap.json');
const { getConnectionString, getQueryByContainerId, getFiltersByCollectionName, mapper } = require('./utils');
const { promises: fsp } = require('fs');
const { ObjectId } = require('mongodb');

const logFileNamePrefix = 'cosmos-to-mongo-migration';

module.exports = class MigrationService {
	constructor(collection) {
		this.collection = collection;
		this.logFileName = `${__dirname}/logs/${logFileNamePrefix}-${collection}.log`;
		this.errorLogFileName = `${__dirname}/errorLogs/${logFileNamePrefix}-${collection}.log`;
	}

	async initServices() {
		// Initialize Cosmos DB client
		this.cosmosClient = new CosmosClient({
			endpoint: CosmosConfig.endpoint,
			key: CosmosConfig.key,
		});

		this.mongoClient = new MongoClient(getConnectionString(MongoConfig), {
			serverApi: {
				version: ServerApiVersion.v1,
				strict: true,
				deprecationErrors: true,
			},
		});

	}

	async checkMongoConnection() {
		try {
			await this.mongoClient.connect();
			console.log('✅ MongoDB is connected successfully!');
		} catch (error) {
			console.error('❌ MongoDB connection failed:', error);
		} finally {
			await this.mongoClient.close();
		}
	}

	async migrateCollection(containerId) {
		const cosmosContainer = this.cosmosClient.database(CosmosConfig.databaseName).container(containerId);
		let continuationToken;
		let items = [];
		// Reading items in batches
		do {
			const query = getQueryByContainerId(containerId);
			console.log('cosmos query -->', query)
			const { resources, continuation } = await cosmosContainer.items
				.query({
					query,
					parameters: [],
				}, {
					maxItemCount: 1000,
					continuationToken
				})
				.fetchNext();
			items = [...resources];
			this.docCount += items.length;
			await this.bulkUpsertToMongoDB({ collectionName: containerId, items });
			//await this.insertMongo({ collectionName: containerId, items });
			continuationToken = continuation;
			console.log('continuationToken--', continuationToken);
			console.log('continuation --', continuation);
			console.log('items.length ---> ', items.length);
		} while (continuationToken && items.length <= 1000);
	}

	async insertMongo({ collectionName, items }) {
		try {
			//const uri = "mongodb+srv://mkpeventsmongodb-devadmin:T%40tsDCqU5Ktq@mkpeventsmongodb-dev-pl-0.xjprc.mongodb.net/offerservicelocal?retryWrites=true&w=majority"; // Replace with your MongoDB connection string
			//const client = new MongoClient(uri);

			//await client.connect();
			//const database = client.db("offerservicelocal"); 
			//const collection1 = database.collection("FbcStock");

			const collection = this.mongoClient
				.db(MongoConfig.databaseName)
				.collection(collectionName);

			const newDoc = {
				name: "John Doe",
				age: 30,
				email: "john.doe@example.com",
				createdAt: new Date()
			};

			const result =  await collection.insertOne(newDoc);
			console.log('single document inserted is:----', result);

			//const query = { shopId: "25517" }; // Change to your query criteria
			//const document = await collection.findOne(query);

			//console.log("Found document:", document);

		} catch (error) {
			console.log('error while saving single document :----', error);
			fsp.appendFile(this.errorLogFileName, `\nError during bulk upsert: ${JSON.stringify(error)}`);
		}
	}

	async bulkUpsertToMongoDB({ collectionName, items }) {
		if (items.length === 0) return;
		const collection = this.mongoClient
			.db(MongoConfig.databaseName)
			.collection(collectionName);

		// Creating bulk operations
		const bulkOps = items.map((item) => {
			//console.log('Current item --> ', item);
		    const _id = item.id;
			//console.log('_id for mongo....', _id);
		    item = mapper(collectionName, item);
			const filter = getFiltersByCollectionName(collectionName, {...item });
			//console.log('Filter for collection:----', filter);
			//console.log('Item to insert --> ', item);
		    return {
		        updateOne: {
		            filter: filter,
		            update: {
		                $setOnInsert:  { },
		                $set: { ...item }
		            },
		            upsert: true,
		        },
		    };
		});

		try {
			// Perform bulk upsert
			//console.log('collection is:----', collection);
			const result = await collection.bulkWrite(bulkOps);
			var a = `\nBulk upsert complete for ${collectionName}: ${result.modifiedCount} modified, ${result.upsertedCount} upserted, ${result.insertedCount} inserted, ${result.matchedCount} matched.`;
			console.log(a);
			await fsp.appendFile(this.logFileName, `\nBulk upsert complete for ${collectionName}: ${result.modifiedCount} modified, ${result.upsertedCount} upserted, ${result.insertedCount} inserted, ${result.matchedCount} matched.`);
		} catch (error) {
			await fsp.appendFile(this.errorLogFileName, `\nError during bulk upsert: ${JSON.stringify(error)}`);
		}
	}

	async execute() {
		try {
			console.log('Starting migration...');
			await this.initServices();

			//this.checkMongoConnection();
			console.log('All dependencies initialized.');
			await fsp.appendFile(this.logFileName, `\nMigrating collection:, ${this.collection}.\nStarted at: ${new Date().toISOString()}`);
			console.time(`Time taken in migration ${this.collection} collection`);
			this.docCount = 0;
			await this.migrateCollection(this.collection);
			await fsp.appendFile(this.logFileName, `\nTotal documents migrated for ${this.collection}: ${this.docCount}. \nCompleted at: ${new Date().toISOString()}`);
			console.log(`Total documents migrated for ${this.collection}: ${this.docCount}`);
			console.timeEnd(`Time taken in migration ${this.collection} collection`);
			return;
		} catch (err) {
			await fsp.appendFile(this.errorLogFileName, `\nError migrating collection: ${JSON.stringify(err)}`);
		}
	}
};
