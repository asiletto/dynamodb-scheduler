const DynamoScheduler = require("./dscheduler.js")

scheduler = new DynamoScheduler("us-east-2","http://localhost:8000","scheduler");

var errCallback = function(err){
	console.error("Unable to add item. Error JSON:", JSON.stringify(err, null, 2));
}

var okCallback = function(data){
	 console.log("Added item:", JSON.stringify(data, null, 2));
}

scheduler.schedule("001", {"somekey":"somevalue"}, Date.now() + (1000 * 10), errCallback, okCallback);