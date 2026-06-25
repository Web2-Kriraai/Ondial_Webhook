const { MongoClient } = require('mongodb');

const uri = "mongodb://ondialai:8R1svRIXexM970Om@ac-ruo8prr-shard-00-00.8ks8wj6.mongodb.net:27017,ac-ruo8prr-shard-00-01.8ks8wj6.mongodb.net:27017,ac-ruo8prr-shard-00-02.8ks8wj6.mongodb.net:27017/ondial?authSource=admin&replicaSet=atlas-mnqq8l-shard-0&retryWrites=true&w=majority&ssl=true";

async function main() {
    const client = new MongoClient(uri);
    try {
        await client.connect();
        const db = client.db('ondial');
        const callLog = await db.collection('CallLogs').findOne({
            $or: [
                { call_id: '76fa4422-9360-4c4a-90be-976ecee279df' },
                { lead_id: '76fa4422-9360-4c4a-90be-976ecee279df' },
                { "twilio.call_sid": '76fa4422-9360-4c4a-90be-976ecee279df' }
            ]
        });
        console.log("=== CALL LOG ===");
        console.log(JSON.stringify(callLog, null, 2));

        if (callLog && callLog.campaign_id) {
            const { ObjectId } = require('mongodb');
            const campaign = await db.collection('campaigns').findOne({ _id: new ObjectId(callLog.campaign_id) });
            console.log("=== CAMPAIGN ===");
            console.log(JSON.stringify(campaign, null, 2));
        }
    } catch (err) {
        console.error("Error:", err);
    } finally {
        await client.close();
    }
}

main();
