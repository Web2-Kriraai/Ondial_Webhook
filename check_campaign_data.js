const { MongoClient, ObjectId } = require('mongodb');

const uri = "mongodb://ondial__db:nf5IdE3JP63tXBFZ@ac-nlfhhjp-shard-00-00.ak2rgbl.mongodb.net:27017,ac-nlfhhjp-shard-00-01.ak2rgbl.mongodb.net:27017,ac-nlfhhjp-shard-00-02.ak2rgbl.mongodb.net:27017/ondial_test?ssl=true&replicaSet=atlas-16ketm-shard-0&authSource=admin&appName=ondial";

async function main() {
    const client = new MongoClient(uri);
    try {
        await client.connect();
        
        for (const dbName of ['ondial', 'ondial_test']) {
            console.log(`--- Checking Database: ${dbName} ---`);
            const db = client.db(dbName);
            
            const campaign = await db.collection('campaigns').findOne({ _id: new ObjectId('6a3665262e84a104a57d7a39') });
            if (campaign) {
                console.log("=== Campaign Found ===");
                console.log("Name:", campaign.campaignName || campaign.name);
                console.log("Status:", campaign.status);
                console.log("SubServiceId:", campaign.campaignServiceSubId);
                
                // Check CallLogs
                const callLogsCount = await db.collection('CallLogs').countDocuments({ campaign_id: '6a3665262e84a104a57d7a39' });
                const callLogsCountObjId = await db.collection('CallLogs').countDocuments({ campaign_id: new ObjectId('6a3665262e84a104a57d7a39') });
                console.log("CallLogs count (string campaign_id):", callLogsCount);
                console.log("CallLogs count (ObjectId campaign_id):", callLogsCountObjId);
                
                // Check Call Analysis records
                const callAnalysisCountStr = await db.collection('call_analysis').countDocuments({ campaign_id: '6a3665262e84a104a57d7a39' });
                const callAnalysisCountObj = await db.collection('call_analysis').countDocuments({ campaign_id: new ObjectId('6a3665262e84a104a57d7a39') });
                console.log("Call Analysis count (string campaign_id):", callAnalysisCountStr);
                console.log("Call Analysis count (ObjectId campaign_id):", callAnalysisCountObj);
                
                // Let's print some Call Analysis documents
                const sampleAnalysis = await db.collection('call_analysis').find({
                    $or: [
                        { campaign_id: '6a3665262e84a104a57d7a39' },
                        { campaign_id: new ObjectId('6a3665262e84a104a57d7a39') }
                    ]
                }).limit(5).toArray();
                console.log("=== Sample Call Analysis Documents ===");
                console.log(JSON.stringify(sampleAnalysis, null, 2));
                
                break;
            } else {
                console.log("Campaign not found in this DB.");
            }
        }
    } catch (err) {
        console.error("Error:", err);
    } finally {
        await client.close();
    }
}

main();
