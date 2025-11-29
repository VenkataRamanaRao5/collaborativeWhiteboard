const cluster = require('cluster'),
      express = require('express'),
      moment = require('moment'),
      fs = require('fs'),
      MongoClient = require('mongodb').MongoClient,
      fabric = require('fabric').fabric,
      // Use the new Redis client for History features
      { createClient } = require('redis'), 
      port = process.env.PORT || 8080, // Azure uses process.env.PORT
      numCPUs = require('os').cpus().length,
      printLines = '----------------------------------------------------';

// JSON to store whiteboard state (Legacy local file)
const whiteboardJsonFile = __dirname + '/public/whiteboard.json';
fs.writeFile(whiteboardJsonFile, '', ()=> console.log('Whiteboard JSON file created...'));

// Mongodb connection string (From Azure App Settings)
const mongodbUri = process.env.MONGO_URI;
const redisUrl = process.env.REDIS_URL;

// Retrieve & send whiteboard state
var whiteboardContent = fs.readFileSync(whiteboardJsonFile, 'utf-8');

// Store connected clients
let leader = null;

// store worker threads
let workers = [];
  
if (cluster.isMaster) {
  // Build master http server that doesn't listen for port connections 
  var server = require('http').createServer();
  var io = require('socket.io')(server);
  var redisAdapter = require('socket.io-redis');

  // Attach redis adapter to master socket instance
  // Ensure your REDIS_URL is in format: redis://:password@host:port
  io.adapter(redisAdapter(redisUrl));

  // Store clients details
  let allClients = new Map();
  let allUsers = {};
  var socketCounter = 1;

  console.log(`Master process initializing ${numCPUs} workers`);
  // createListing(whiteboardContent);
  
  // Initialize worker processes
  for (var i = 0; i < numCPUs; i++) {
    // Create & store worker server
    workers.push(cluster.fork());

    // Listen for messages from worker
    workers[i].on('message', (msg)=> {
      
      // Client joins
      if (msg.userJoined) {
        // Retrieve socket id & username
        var socketId = msg.userJoined[0];
        var username = msg.userJoined[1];

        // Select initial leader
        if(allClients.size < 1) {
          leader = socketId;
          
          console.log(`\n${printLines}\nMaster Server: ${socketId} set as leader`);

          // Get leader's whiteboard state
          retrieveWhiteboards(io, leader);
        } else {
            // Note: With Redis History Replay added below, 
            // relying on the leader for the initial state is less critical, 
            // but we keep it here for your existing save functionality.
            io.to(leader).emit('canvas:leader', socketId);
          console.log(`\n${printLines}\nMaster Server: Canvas state sent to ${socketId}`);
        }
        // Store socket & username
        allClients.set(msg.userJoined[0], socketCounter);
        allUsers[msg.userJoined[0]] = msg.userJoined[1];
        socketCounter++;

        // Send status update to all clients 
        io.emit('update:history', `${username} joined.`);
        io.emit('update:userCount', getUserCount(allUsers));
        io.emit('update:users', allUsers);
      }
      // Leader loads previous canvas
      else if (msg.canvasLoad) {
        // Retrieve socket id
        var socketId = msg.canvasLoad[0];
        var previousCanvas = msg.canvasLoad[1];

        // Send previous canvas state to clients
        io.emit('canvas:load', previousCanvas);
        io.emit('update:history', `${allUsers[socketId]} loaded saved whiteboard.`);
        console.log(`\n${printLines}\nMaster Server: '${socketId}' loaded saved whiteboard`);
      }
      // Client clears whiteboard
      else if (msg.canvasClear) {
        // Retrieve socket id
        var socketId = msg.canvasClear;
        
        // CLEAR REDIS HISTORY TOO
        // We need to signal workers to clear redis, but for now 
        // we just emit the event. (Ideally, clear 'whiteboard_history' in Redis here)
        io.emit('canvas:clear');
        
        io.emit('update:history', `${allUsers[socketId]} cleared the whiteboard.`);
        console.log(`\n${printLines}\nMaster Server: '${socketId}' cleared the whiteboard`);
      }
      // Leader closes whiteboard
      else if (msg.canvasClose) {
        // Retrieve socket id
        var socketId = msg.canvasClose;

        // Close all sockets
        io.emit('canvas:close');
        io.close();
        console.log(`\n${printLines}\nMaster Server: '${socketId}' closed the whiteboard`);
      }
      // Client disconnects
      else if (msg.userDisconnected) {
        // Retrieve socket id
        var socketId = msg.userDisconnected;
        var emitMessage = ``;

        // Leader client disconnects
        if(socketId === leader) {
          console.log(`\n${printLines}\nMaster Server: leader ${socketId} left`);
          emitMessage = `${allUsers[socketId]} leader left.`;
          leader = null;
        } else {
          console.log(`\n${printLines}\nMaster Server: ${socketId} left.`);
          emitMessage = `${allUsers[socketId]} left.`;
        }
        // Delete client socket
        allClients.delete(msg.userDisconnected);
        delete allUsers[msg.userDisconnected];

        var randomNumber = getRandomNumber(0, getUserCount(allUsers)) - 1;

        // Select initial leader
        if(allClients.size < 2) {
            if(Object.keys(allUsers).length > 0) {
                 leader = Object.keys(allUsers)[0];
          
          console.log(`\n${printLines}\nMaster Server: ${leader} set as leader`);

          // Get leader's whiteboard state & set new leader
                 retrieveWhiteboards(io, leader);
            }
        } else {
          leader = Object.keys(allUsers)[randomNumber];
          console.log(`\n${printLines}\nMaster Server: ${leader} set as leader`);

          // Get leader's whiteboard state & set new leader
          retrieveWhiteboards(io, leader);
        }
        // Send status update to all clients 
        io.emit('update:history', emitMessage);
        io.emit('update:userCount', getUserCount(allUsers));
        io.emit('update:users', allUsers);
      } 
    });
  }

  // Worker process exits
  cluster.on('exit', function(worker, code, signal) {
    console.log('Worker ' + worker.process.pid + ' died');

    // Replace dead worker
    workers.push(cluster.fork());
  }); 

} else {
  // build worker server using express & http
  var app = express();
  var server = require('http').createServer(app);
  var io = require('socket.io')(server);
  var redisAdapter = require('socket.io-redis');

  // Attach Redis adapter
  io.adapter(redisAdapter(redisUrl));

  // --- NEW: Dedicated Redis Client for History Replay ---
  const historyClient = createClient({ url: redisUrl });
  
  historyClient.on('error', (err) => console.log('Redis Client Error', err));

  (async () => {
    try {
        await historyClient.connect();
        console.log(`Worker ${process.pid}: Connected to Redis for History`);
    } catch (e) {
        console.error(`Worker ${process.pid}: Redis Connection Failed`, e);
    }
  })();

  server.listen(port, ()=> console.log(`Worker Server ${process.pid}: listening on port ${port}...`));

  // set server directory & PORT
  app.use(express.static(__dirname));

  app.get('/', (request,response)=>{
    response.setHeader('Content-Type', 'text/html');
    response.sendFile(__dirname + '/public/whiteboard.html');
  });

  io.sockets.on('connection', async (socket)=> {
    let workerClients = new Map();
    let workerUsers = {};
    var i = 1;

    console.log(`Worker Server ${process.pid}: [id=${socket.id}] connected...`);

    // --- NEW: REPLAY HISTORY ON CONNECT ---
    // Immediately send the stored drawing history to the new user
    try {
        const history = await historyClient.lRange('whiteboard_history', 0, -1);
        if(history && history.length > 0) {
            console.log(`Replaying ${history.length} events to ${socket.id}`);
            history.forEach(item => {
                socket.emit('object:added', JSON.parse(item));
            });
        }
    } catch (err) {
        console.error("Error replaying history:", err);
    }

    socket.on('join', (username)=> {
      // Store client socket & username
      workerClients.set(socket, i);
      workerUsers[socket.id] = username;
      i++;

      // Send message to master process
      process.send({ userJoined: [socket.id, username] });

      // Update all clients on join
      socket.emit('update:history', `${username} connected to server.`);
    });

    // --- MODIFIED: Object added by client ---
    socket.on('object:added', async (data)=> {
        // 1. Broadcast to everyone else (Standard logic)
        socket.broadcast.emit('object:added', data);

        // 2. NEW: Save to Redis History
        try {
            await historyClient.rPush('whiteboard_history', JSON.stringify(data));
        } catch (err) {
            console.error("Failed to save to history:", err);
        }
    });
    
    socket.on('canvas:leader', (data)=> {
      // Store canvas locally
      fs.writeFile(whiteboardJsonFile, data[1], ()=> console.log(`Worker Server ${process.pid}: Leader's whiteboard state saved.`));

      // Send leader's canvas state to new client
      io.to(data[0]).emit('canvas:initial', data[1]);
      
      // Save snapshot to MongoDB
      var savedWhiteboard = JSON.parse(data[1]);
      savedWhiteboard["timestamp"] = moment().format()
      createWhiteboardSave(savedWhiteboard);
    });

    // Leader loads previous canvas state
    socket.on('canvas:load', (data)=> {
      // Send previous canvas state to clients
      process.send({ canvasLoad: [socket.id, data] });
    });

    // Whiteboard closed by leader
    socket.on('canvas:close', ()=> {
      // Inform master of whiteboard close
      process.send({ canvasClose: socket.id });
      io.close();
    });

    socket.on('canvas:clear', async ()=> {
      // Clear local board
      process.send({ canvasClear: socket.id });
      
      // NEW: Clear Redis History
      try {
          await historyClient.del('whiteboard_history');
      } catch(err) { console.error(err); }
    });

    // Client disconnects
    socket.on('disconnect', ()=> {
      // Send message only if user fully joined the whiteboard
      if (socket.id in workerUsers){
        // Update master process
        process.send({ userDisconnected: socket.id });

        // Remove client socket & update user list
        workerClients.delete(socket);
        delete workerUsers[socket.id];

        console.log(`Worker Server ${process.pid}: ${socket.id} left.`);
      } else {
        console.log(`Worker Server ${process.pid}: ${socket.id} disconnected...`);
      }
    });
  });

} // End of else

// --- HELPER FUNCTIONS ---

function getUserCount(object){ return Object.keys(object).length; }

function getRandomNumber(min, max) { 
  min = Math.ceil(min);
  max = Math.floor(max);
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

// Save to MongoDB
async function createWhiteboardSave(whiteboardState){
  if (!mongodbUri) return; // Guard against missing Env Var

  const client = await MongoClient.connect(mongodbUri, { useNewUrlParser: true, useUnifiedTopology: true }).catch(err => { console.log(err);});
  if(!client) return; 

  // Store whiteboard on database
  try {
    const result = await client.db("collaborativeWhiteboard").collection("whiteboardStates").insertOne(whiteboardState);
    console.log(`Whiteboard state stored with id: ${result.insertedId}`);
  } catch (err) {
    console.log(err);
  } finally {
    client.close();
  }
}

// Function to retrieve saved whiteboard states from database 
async function retrieveWhiteboards(io, socketId){
  if (!mongodbUri) return;

  const client = await MongoClient.connect(mongodbUri, { useNewUrlParser: true, useUnifiedTopology: true }).catch(err => { console.log(err);});
  if(!client) return; 

  // Retrieve whiteboard states from database
  try {
    const cursor = client.db("collaborativeWhiteboard").collection("whiteboardStates").find().sort({_id:-1}).limit(5);
    const result = await cursor.toArray();

    // Send saved whiteboards to leader
    io.to(socketId).emit('set:leader', result);
  } catch (err) {
    console.log(err);
  } finally {
    client.close();
  }
}