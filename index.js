const express = require('express');
const kue = require('kue');
const bodyParser = require('body-parser');
const got = require('got');
const kueUiExpress = require('kue-ui-express');
const app = express();
const chalk = require('chalk');

const queue = kue.createQueue();
const port = 9090;

app.use(bodyParser.json());
kueUiExpress(app, '/kue/', '/kue-api');
app.use('/kue-api/', kue.app);

app.post('/', (req, res) => {
	var data = req.body;
	console.log(chalk.yellow('\nreq.body =>'), req.body);
	var seconds = 0;
	if (data.task.scheduleTime.seconds) {
		var currentTime = Math.floor(new Date().getTime() / 1000);
		seconds = data.task.scheduleTime.seconds - currentTime;
	}

	var job = queue.create('taskqueue', data);
	if (seconds > 0) {
		console.log(chalk.cyan(`\nDelay by ${seconds}s`));
		job.delay(seconds * 1000);
	}
	job.save(function(err) {
		if (!err) {
			console.log(chalk.cyanBright(`\nQueued job with id => ${job.id}`));
			return res.status(200).json({ id: job.id });
		}
		return res.status(500).json({});
	});
});

app.delete('/:task_id', (req, res) => {
	kue.Job.remove(req.params.task_id, (err) => {
		if (!err) {
			console.log(chalk.cyanBright(`\nDeleted job id => ${req.params.task_id}`));
			return res.status(200).json({});
		}
		res.status(200).json({});
	});
});

queue.process('taskqueue', function(job, done) {
	console.log(chalk.cyanBright(`\nExecuting job => ${job.id}`));
	console.log(chalk.yellow('\njob.data =>'), job.data)
	const appEngineHttpRequest = job.data.task.appEngineHttpRequest;
	const queueName = job.data.parent.queueName;
	const headers = { 'x-appengine-queuename': queueName, 'x-appengine-taskname': job.id.toString() };
	const options = { baseUrl: "http://localhost:5020", method: appEngineHttpRequest?.httpMethod || 'POST' };
	if(!appEngineHttpRequest.relativeUri) {
		return done(new Error('relativeUri is required'));
	}

	if (appEngineHttpRequest.body) {
		options.body = Buffer.from(appEngineHttpRequest.body, 'base64').toString('utf-8');
		headers['Content-Type'] = 'application/octet-stream';
	}
	options.headers = headers;
	console.log(chalk.yellow('\nExecuting =>'), appEngineHttpRequest.relativeUri, options);

	got(appEngineHttpRequest.relativeUri, options)
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
	console.error(err);
});

app.listen(port, () => console.log(`Queue listening on port ${port}!`));