const { PubSub } = require('@google-cloud/pubsub');

// Instantiates a client

const pubsub = new PubSub();
exports.publishMessage = async (req, res) => {

    res.set('Access-Control-Allow-Origin', '*');
    res.set('Access-Control-Allow-Methods', 'GET, PUT, POST, OPTIONS');

    res.set('Access-Control-Allow-Headers', '*');
    if (req.method === 'OPTIONS') {
        res.end();
    }
    console.log("req=", req);
    if (!req.body.topic || !req.body.message) {

        res.status(400)
            .send('Missing parameter(s); include "topic" and "message" properties in your request.Updated');
        return;

    }
    console.log(`Publishing message to topic`);
    // References an existing topic
    const topic = pubsub.topic(req.body.topic);
    console.log('creating message object');
    //const message = req.body.message;
    const { id, message } = req.body;
    const messageBuffer = Buffer.from(JSON.stringify({ id, message }), 'utf8');
    console.log(`Publishing message to MessageBuffer`);
    // Publishes a message
    try {
        await topic.publish(messageBuffer);
        console.log(`sending a response`);
        res.status(200).send('Message published successfully');
    } catch (err) {
        console.error(err);
        res.status(500).send(err);
        return Promise.reject(err);
    }

};