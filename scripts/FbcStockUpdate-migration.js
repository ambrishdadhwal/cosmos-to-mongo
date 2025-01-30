const config = require("../config");
const { collections } = require("../constants");
const MigrationService = require("../migrationService");


(async () => {
    const migrationInstance = new MigrationService(collections.FbcStockUpdate);
    await migrationInstance.execute();
    process.exit(0);
    return;
})()