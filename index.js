const { Pool } = require('pg');
const MongoClient = require('mongodb').MongoClient;
const uri = 'mongodb://localhost:27018';

let mongodb = 'twitter'

const pool = new Pool({
    user: 'postgres',
    host: 'localhost',
    database: 'twitter',
    password: 'postgres',
    port: 5433
});

pool.on('error', (err, client) => {
    console.error('Unexpected error on idle client', err);
    process.exit(-1);
});

const clientMongo = new MongoClient(uri, {
    useUnifiedTopology: true,
});

clientMongo.connect(function (err, db) {
    console.log('Connected to MongoDB');
    if (err)
        db.close();
});

async function loadTweets() {
    const db = await clientMongo.db(mongodb);
    const clientPg = await pool.connect();

    if (clientPg === undefined)
        return console.error('Error acquiring client');
    try {
        console.log('Starting ...')
        const queryText = `SELECT t.id, t.content, t.happened_at, t.author_id, t.parent_id, ARRAY_AGG (h.value) hashtagss
                           FROM tweets t
                           JOIN tweet_hashtags th ON th.tweet_id = t.id
                           JOIN hashtags h ON h.id = th.hashtag_id
                           WHERE t.compound < -0.5 OR t.compound > 0.5
                           GROUP BY t.id
                           OFFSET $1
                           LIMIT 5000`;

        for(let i = 0; i < 675000; i += 5000) {
            console.log(i);
            let result = await clientPg.query(queryText, [i]);

            try {
                let cursor = await db.collection('tweets').insertMany(result.rows);
            } catch (e) {
                throw e;
            }
        }
        
        console.log("exiting loadTweets()");
    } catch (e) {
        throw e;
    } finally {
        clientPg.release();
    }
}

async function loadAccounts() {
    const db = await clientMongo.db(mongodb);
    const clientPg = await pool.connect();

    if (clientPg === undefined)
        return console.error('Error acquiring client');
    try {
        console.log('Starting ...')
        const queryText = `SELECT a.*
                           FROM (SELECT DISTINCT a.id
                                 FROM accounts a
                                 JOIN tweets t ON t.author_id = a.id
                                 WHERE t.compound < -0.5 OR t.compound > 0.5) accs
                           JOIN accounts a ON a.id = accs.id
                           OFFSET $1
                           LIMIT 10000;`;

        for(let i = 0; i < 140000; i += 10000) {
            console.log(i);
            let result = await clientPg.query(queryText, [i]);

            try {
                let cursor = await db.collection('accounts').insertMany(result.rows);
            } catch (e) {
                throw e;
            }
        }
        
        console.log("exiting loadAccounts()");
    } catch (e) {
        throw e;
    } finally {
        clientPg.release();
    }
}

async function lastTen() {
    const db = await clientMongo.db(mongodb);

    try {
        let acc = await db.collection('accounts').findOne({screen_name: 'Marndin12'})
        let acc_id = acc['id']
        const tweets = await db.collection('tweets').find({author_id: '3003720760'}).sort({happened_at: 1}).limit(10).toArray()
        acc['tweets'] = tweets
        return acc
    } catch(e) {
        throw e
    }
}

async function firstTen() {
    const db = await clientMongo.db(mongodb);

    try {
        const tweets =  await db.collection('tweets').find({parent_id: '1246874043682299904'}).sort({happened_at: -1}).limit(10).toArray()
        for(const i of tweets) {
            const author = await db.collection('accounts').findOne({id: i.author_id})
            i['author'] = author.name
        }
        return tweets
    } catch(e) {
        throw e
    }
}

async function startProgram() {
    // await loadTweets();
    // await loadAccounts();
    let last_ = await lastTen();
    let first_ = await firstTen();
    console.log(last_)
    console.log(first_)
    
    console.log('Exit');
    process.exit(1);
}

startProgram();