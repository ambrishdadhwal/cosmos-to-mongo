module.exports = {
    cosmos: {
        endpoint: process.env.COSMOS_DB_ENDPOINT,
        key: process.env.COSMOS_DB_KEY,
        databaseName: process.env.COSMOS_DB_DB_NAME,
    },
    mongo: {
        host: process.env.MONGO_DB_HOST,
        username: process.env.MONGO_DB_USERNAME,
        password: process.env.MONGO_DB_PASSWORD,
        databaseName: process.env.MONGO_DB_DB_NAME,
    },
    BATCH_SIZE: parseInt(process.env.BATCH_SIZE || '10', 10),
    START_TIME: process.env.START_TIME,
}