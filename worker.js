//input
const DynamoScheduler = require("./dscheduler.js")
const region="us-east-2";
const endpoint="http://localhost:8000";
const schedulerTableName="scheduler";
const scheduleIntervalMs=1000;

//output
const amqplib = require('amqplib');
const amqp = require('amqplib/callback_api');
const mquser="root";
const mqpass="pippi";
const mqhost="amqp://localhost";
const destinationQueue = 'jobs';

scheduler = new DynamoScheduler(region,endpoint,schedulerTableName);

const opt = { credentials: amqplib.credentials.plain(mquser, mqpass) };

var errCallback = function(err){
	console.error("Unable to select jobs. Error JSON:", JSON.stringify(err, null, 2));
}

amqp.connect(mqhost, opt, function(error0, connection) {
  if (error0) {
    throw error0;
  }
  connection.createChannel(function(error1, channel) {
    if (error1) {
      throw error1;
    }

    channel.assertQueue(destinationQueue, {
      durable: false
    });
	
	setInterval(function(){
		scheduler.processExpiredJobs("001", errCallback, function(item){
			channel.sendToQueue(destinationQueue, Buffer.from(JSON.stringify(item)));
			console.log(" ["+item.expire+"] Sent message to queue: " + destinationQueue);
		});	
	}, scheduleIntervalMs);
	
  });
});

