
/** Extract large number of data from Postgresql using pg-cursor "without loading the entire result-set into memory"*/
/** Load data into Mongodb and/or AWS-S3 */


const { Pool } = require('pg')
const Cursor = require('pg-cursor')
const {MongoClient} = require('mongodb')

var AWS = require('aws-sdk');

AWS.config.update({
    region: 'us-west-1',
    accessKeyId: '',
    secretAccessKey: '',
    
});

var s3 = new AWS.S3(); 

const uri = "mongodb+srv://..."
const clientMongo = new MongoClient(uri);

var loader = async (mongo_data)=>{
    try {
        const mongoConnection = await clientMongo.connect();
        
        async function loadRecords(clientMongo, record){
            
            const result = await clientMongo.db("db_name").collection("col_name").insertMany(record);
            
        }

        loadRecords(clientMongo,mongo_data)
            
    } catch (error) {
        throw error
    }
}

var callCursor = async () =>{

    const pool = new Pool({
        user: 'postgres',
        host: '',
        database: '',
        password: '',
        port: 5432,
    })

    const client = await pool.connect()

    const cursor = await client.query(new Cursor('Select * From ...'))
    
    var s3Ops = (records)=>{
        try {
            today = new Date()
            s3.putObject({
                Bucket: '',
                Key: today+'.json',
                Body: JSON.stringify(records),
                ContentType: "application/json"
                },
                function (err,data) {
                  console.log(JSON.stringify(err) + " " + JSON.stringify(data));
                }
            );
            
        } catch (error) {
            throw error
        }
    }
    
   
    
    var reCursor = async () => {
        await cursor.read(100, (err, rows) => {
        
            if(err){throw err}

            if(!rows.length){
                cursor.close(() => {
                    
                    client.release()
                    console.log("client released")
                    
                })
                return
                
            }
            
            /**upload to bucket here */
            //s3Ops(rows) (will generate lots of files in bucket)

            /** upload to mongodb  here*/
            loader(rows)
            
            reCursor()
        })
       
        console.log("Rolling ...")
    }
    reCursor()
    
    
    return
}
