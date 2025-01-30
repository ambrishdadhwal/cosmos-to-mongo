const config = require("../config");
const { collections } = require("../constants");
const MigrationService = require("../migrationService");


(async () => {
    const migrationInstance = new MigrationService(collections.offerMessageRecord);
    await migrationInstance.execute();
    process.exit(0);
    return;
})()