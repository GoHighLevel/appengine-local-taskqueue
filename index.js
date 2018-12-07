const express = require('express');
const kue = require('kue');
const bodyParser = require('body-parser');
const got = require('got');
const kueUiExpress = require('kue-ui-express');
const app = express();

const queue = kue.createQueue();

const port = 9090;

app.use(bodyParser.json());
kueUiExpress(app, '/kue/', '/kue-api');
app.use('/kue-api/', kue.app);
app.post('/', (req, res) => {
	var data = req.body;
	console.log(req.body);
	var seconds = 0;
	if (data.task.scheduleTime.seconds) {
		var currentTime = Math.floor(new Date().getTime() / 1000);
		seconds = data.task.scheduleTime.seconds - currentTime;
	}

	var job = queue.create('taskqueue', data);
	if (seconds > 0) {
		console.log(`Delay by ${seconds}s`);
		job.delay(seconds * 1000);
	}
	job.save(function(err) {
		if (!err) {
			console.log(`Queued job with id => ${job.id}`);
			return res.status(200).json({ id: job.id });
		}
		return res.status(500).json({});
	});
});
app.delete('/:task_id', (req, res) => {
	kue.Job.remove(req.params.task_id, (err) => {
		if (!err) {
			console.log(`Deleted job id => ${req.params.task_id}`);
			return res.status(200).json({});
		}
		res.status(500).json({});
	});
});

queue.process('taskqueue', function(job, done) {
	console.log(`Executing ${job.id}`);
	const appEngineHttpRequest = job.data.task.appEngineHttpRequest;
	const queueName = job.data.parent.queueName;
	const headers = { 'x-appengine-queuename': queueName, 'x-appengine-taskname': job.id.toString() };
	const options = { baseUrl: appEngineHttpRequest.baseUrl, method: appEngineHttpRequest.httpMethod };
	if (appEngineHttpRequest.payload) {
		options.body = Buffer.from(appEngineHttpRequest.payload, 'base64').toString('utf-8');
		headers['Content-Type'] = 'application/octet-stream';
	}
	options.headers = headers;
	console.log(appEngineHttpRequest.relativeUrl, options);
	got(appEngineHttpRequest.relativeUrl, options)
		.then(() => {
			done();
		})
		.catch((err) => {
			done(err);
		});
});

process.once('SIGTERM', function(sig) {
	queue.shutdown(5000, function(err) {
		console.log('Kue shutdown: ', err || '');
		process.exit(0);
	});
});

queue.on('error', function(err) {
	console.errir(err);
});

app.listen(port, () => console.log(`Queue listening on port ${port}!`));
