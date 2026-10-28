import { MongoClient } from 'mongodb';

const uri =
  process.env.MONGO_URI ??
  'mongodb://localhost:27017';

let client;
let clientPromise;

if (!global._mongoClientPromise) {
  client = new MongoClient(uri);
  global._mongoClientPromise = client.connect();
}

clientPromise = global._mongoClientPromise;

export async function getDb() {
  const mongoClient = await clientPromise;
  return mongoClient.db('rutasur');
}