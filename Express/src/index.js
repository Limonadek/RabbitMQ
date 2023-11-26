const express = require('express')
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const amqp = require('amqplib/callback_api');
const cheerio = require('cheerio');
const axios = require('axios');
const app = express()
const port = 3002
const cassandra = require('cassandra-driver');

const client = new cassandra.Client({
    contactPoints: ['127.0.0.1:9042'],
    localDataCenter: 'datacenter1',
    keyspace: 'mykeyspace'
  });


const fetchData = async (url, res) => {
    try {
        const response = await axios.get(url).catch((error) => {
            res.json({ url, success: false, error: error.message });
        });
        const $ = cheerio.load(data);
    
        const requestData = {
            url: url,
            name: $('h1').text(),
            html: response.data
        }

        amqp.connect('amqp://localhost', (err, connection) => {
            if (err) throw err;

            connection.createChannel((err, channel) => {
                if (err) throw err;

                const queue = 'htmlQueue';

                channel.assertQueue(queue, { durable: false });
                channel.sendToQueue(queue, Buffer.from(JSON.stringify(requestData)));
                console.log('Sent data to RabbitMQ');
            });
        });
    } catch (error) {
        return error;
    }
}

const processFile = async (filePath, res) => {
    const fileStream = fs.createReadStream(filePath);
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    for await (const line of rl) {
        fetchData(line, res);
    }
    res.status(200).json({message: "Пользователи распарсились и отправлены в rabbit"});
}

app.post('/parse', async (req, res) => {
    try {
        const urlsHabr = path.resolve(__dirname, 'habr.csv')
        processFile(urlsHabr, res);
    } catch(e) {
        res.status(500).send('Internal Server Error');
    }
})

app.get('/users', async (req, res) => {
    try {
        const query = 'SELECT url, name, html FROM users';
        const result = await client.execute(query);
        res.json(result.rows);
    } catch (error) {
        console.error(error);
        res.status(500).send('Internal Server Error');
    }
});

app.listen(port, () => {
  console.log(`Server start on port ${port}`)
})