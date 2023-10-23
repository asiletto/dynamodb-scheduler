var DynamoScheduler = function(region, endpoint, tablename) {
	this.AWS = require("aws-sdk");
	this.crypto = require('crypto');
	this.tablename = tablename;
	
	this.AWS.config.update({
	  region: region,
	  endpoint: endpoint
	});
	
	this.docClient = new this.AWS.DynamoDB.DocumentClient();
}

DynamoScheduler.prototype.createSchedulerTable(callback){
	tablename = this.tablename;
	
	var tableDefinition = {
	  AttributeDefinitions: [{"AttributeName": "cluster","AttributeType": "S"},{"AttributeName": "expire","AttributeType": "S"}],
	  KeySchema: [{"AttributeName": "cluster","KeyType": "HASH"},{"AttributeName": "expire","KeyType": "RANGE"},],
	  ProvisionedThroughput: {ReadCapacityUnits: 3, WriteCapacityUnits: 3},
	  TableName: tablename,
	  StreamSpecification: {StreamEnabled: false}
	};

	ddb.createTable(tableDefinition, function(err, data) {
	  if (err) {
		debug(" * Error creating "+tablename+" table", err);
	  } else {
		debug(" + table "+tablename+" created");
		callback();
	  }
	});
	
}

DynamoScheduler.prototype.schedule = function(cluster, job, scheduledAt, errCallback, okCallback) {
	
	var randomid = this.crypto.randomBytes(16).toString('hex');

	var params = {
		TableName:this.tablename,
		Item:{
			"cluster":cluster,
			"expire": ""+scheduledAt+"#"+randomid,
			"data":job,
			"status":"SCHEDULED"
		}
	};

	this.docClient.put(params, function(err, data) {
		if (err) {
			errCallback(err);
		} else {
			okCallback(data);
		}
	});
}

DynamoScheduler.prototype.markAcquired = function(cluster, item, processFunction) {
	params = {
		TableName:this.tablename,
		Key:{
			"cluster": cluster,
			"expire": item.expire
		},
		UpdateExpression: "set #status = :status",
		ConditionExpression: "#status = :condExp",
		ExpressionAttributeNames:{
			"#status": "status",
		},
		ExpressionAttributeValues:{
			":status":"AQUIRED",
			":condExp":"SCHEDULED"
		},
		ReturnValues:"UPDATED_NEW"
	};
	console.log(" ["+item.expire+"] marking item as AQUIRED");
	this.docClient.update(params, function(err, data) {
			if(err){
				//TODO
				if(err.code == "ConditionalCheckFailedException")
					console.log(" ["+item.expire+"] trying to process locked job");
			} else {
				console.log(" ["+item.expire+"] item AQUIRED",data);
				processFunction(item);
				
			}
	});
}

DynamoScheduler.prototype.deleteJob = function(cluster, item, errCallback, okCallback) {
	params = {
		TableName:this.tablename,
		Key:{
			"cluster": cluster,
			"expire": item.expire
		},
		ConditionExpression: "#status = :condExp",
		ExpressionAttributeNames:{
			"#status": "status",
		},
		ExpressionAttributeValues:{
			":condExp":"AQUIRED"
		}
	};

	console.log(" ["+ item.expire+"] attempting a conditional delete ");
	
	this.docClient.delete(params, function(err, data) {
		if (err) {
			errCallback(err, item);
		} else {
			okCallback(data, item);
		}
	});

}

DynamoScheduler.prototype.processExpiredJobs = function(cluster, errCallback, processFunction) {
	var params = {
		TableName:this.tablename,
		KeyConditionExpression: "#cluster = :cluster and #expire < :expiretime",
		ExpressionAttributeNames:{
			"#cluster": "cluster",
			"#expire": "expire"
		},
		ExpressionAttributeValues: {
			":cluster": cluster,
			":expiretime" : ""+Date.now()
		}
	};
	
	var self = this;
	this.docClient.query(params, function(err, data) {
		if (err) {
			errCallback(err);
		} else {
			console.log("Query succeeded: " + data.Items.length);
			data.Items.forEach(function(item) {
				
				self.markAcquired(cluster, item, function(){
					try{
						processFunction(item);
					}catch(err){
						console.log(" ["+item.expire+"] error processing job");
					}finally{
						self.deleteJob(cluster, item, function(err, itemDeleted){
								console.log(" ["+itemDeleted.expire+"] error deleting item ", err);
							}, function(data,itemDeleted){
								console.log(" ["+itemDeleted.expire+"] item deleted");
							});
					}
				});
				
			});
		}
	});

}

if (typeof module !== 'undefined') {
	module.exports = DynamoScheduler;
}