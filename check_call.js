const { MongoClient } = require("mongodb");

const uri = "mongodb://ondialai:8R1svRIXexM970Om@ac-ruo8prr-shard-00-00.8ks8wj6.mongodb.net:27017,ac-ruo8prr-shard-00-01.8ks8wj6.mongodb.net:27017,ac-ruo8prr-shard-00-02.8ks8wj6.mongodb.net:27017/ondial?authSource=admin&replicaSet=atlas-mnqq8l-shard-0&retryWrites=true&w=majority&ssl=true";

async function main() {
    const client = new MongoClient(uri);
    try {
        await client.connect();
        const db = client.db("ondial");
        
        const providerCallId = "8abbfd89-315f-49f4-bcb1-0eb80aafc47a";
        const collections = await db.listCollections().toArray();
        
        console.log(`Searching for providerCallId: ${providerCallId} across all collections...`);
        for (const colInfo of collections) {
            const colName = colInfo.name;
            const doc = await db.collection(colName).findOne({
                $or: [
                    { provider_call_id: providerCallId },
                    { providerCallId: providerCallId },
                    { call_id: providerCallId },
                    { callId: providerCallId },
                    { "twilio.call_sid": providerCallId },
                    { "data.provider_call_id": providerCallId }
                ]
            });
            if (doc) {
                console.log(`FOUND in collection: ${colName}`);
                console.log(JSON.stringify(doc, null, 2));
            }
        }

    } catch (e) {
        console.error(e);
    } finally {
        await client.close();
    }
}

main();
