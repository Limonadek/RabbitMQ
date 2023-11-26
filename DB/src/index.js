const amqp = require('amqplib/callback_api');
const cassandra = require('cassandra-driver');

// Подключение к Apache Cassandra
const cassandraClient = new cassandra.Client({
    contactPoints: ['127.0.0.1:9042'],
    localDataCenter: 'datacenter1'
});

const createKeyspaceQuery = `
  CREATE KEYSPACE IF NOT EXISTS mykeyspace
  WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 3
  };
`;

cassandraClient.execute(createKeyspaceQuery)
  .then(() => {
    console.log('Keyspace created successfully.');

    // Использование keyspace
    cassandraClient.execute('USE mykeyspace');

    // Создание таблицы users
    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS users (
        url TEXT PRIMARY KEY,
        name TEXT,
        html TEXT
      );
    `;

    return cassandraClient.execute(createTableQuery);
  })
  .then(() => {
    console.log('Table "users" created successfully.');
  })

amqp.connect('amqp://localhost', (err, connection) => {
  if (err) throw err;

  connection.createChannel((err, channel) => {
    if (err) throw err;

    const queue = 'htmlQueue';

    // Прослушивание очереди для получения данных от первого сервиса
    channel.assertQueue(queue, { durable: false });
    channel.consume(queue, (msg) => {
      const data = JSON.parse(msg.content.toString());
      console.log(data);

      // Обработка данных и запись в базу данных (MongoDB)
      writeToCassandra(data);

      console.log(`Received and processed data for ${data.url}`);
    }, { noAck: true });
  });
});

// Запись данных в Apache Cassandra
async function writeToCassandra(data) {
  const query = 'INSERT INTO users (url, name, html) VALUES (?, ?, ?)';
  const params = [data.url, data.name, data.html];

  await cassandraClient.execute(query, params, { prepare: true });
}

// Обработка ошибок в Cassandra
cassandraClient.on('log', (level, loggerName, message) => {
  console.log(`${level} - ${loggerName}:  ${message}`);
});
console.log("hello");