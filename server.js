// Import required modules
require('dotenv').config();
const express = require('express');
const cors = require('cors');
// Import AWS SDK v3 S3 client and commands
const { S3Client, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');

// --- Configuration ---

// Initialize Express app
const app = express();
const PORT = process.env.PORT || 3000;

// AWS S3 Configuration
// CRITICAL: Ensure your AWS credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
// and region (AWS_REGION) are set in your environment variables.
// DO NOT hard-code credentials here.
const S3_BUCKET_NAME = process.env.S3_BUCKET_NAME || 'daily-weather-output-mscac'; // <-- REPLACE with your bucket name
const S3_REGION = process.env.AWS_REGION || 'us-east-2'; // <-- REPLACE with your bucket's region

// Define S3 object keys
// This is the file your Lambda function creates daily
const WEATHER_FILE_KEY = 'daily-data/prediction.json'; 
// This is the "folder" where subscription emails will be stored
const SUBSCRIPTIONS_PATH = 'subscribers/'; 

// Initialize the S3 Client
// The client will automatically pick up credentials from environment variables
const s3Client = new S3Client({ region: S3_REGION });

// --- Middleware ---

// Enable Cross-Origin Resource Sharing (CORS)
// This allows your frontend (on a different origin) to talk to this backend
app.use(cors());

// Enable Express to parse JSON request bodies
app.use(express.json());

// --- Helper Function ---

/**
 * Helper to stream S3 object content to a string
 * @param {ReadableStream} stream - The S3 object body stream
 * @returns {Promise<string>} - A promise that resolves to the string content
 */
const streamToString = (stream) => {
    return new Promise((resolve, reject) => {
        const chunks = [];
        stream.on('data', (chunk) => chunks.push(chunk));
        stream.on('error', reject);
        stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
    });
};

// --- API Endpoints ---

/**
 * [GET] /weather
 * Fetches the cached weather prediction file from S3.
 */
app.get('/weather', async (req, res) => {
    console.log(`Fetching weather from s3://${S3_BUCKET_NAME}/${WEATHER_FILE_KEY}`);

    const getObjectParams = {
        Bucket: S3_BUCKET_NAME,
        Key: WEATHER_FILE_KEY,
    };

    try {
        // Create and send the GetObjectCommand
        const command = new GetObjectCommand(getObjectParams);
        const s3Response = await s3Client.send(command);

        // Stream the body content to a string
        const contentString = await streamToString(s3Response.Body);
        
        // Parse the JSON content
        const weatherData = JSON.parse(contentString);

        // Send the parsed data to the frontend
        // We assume the file contains {"weatherCode": 0}
        res.status(200).json(weatherData);

    } catch (error) {
        console.error('S3 GetObject error:', error);
        if (error.name === 'NoSuchKey') {
            res.status(404).json({ message: 'Weather prediction file not found.' });
        } else {
            res.status(500).json({ message: 'Failed to fetch weather data.', error: error.message });
        }
    }
});

/**
 * [POST] /subscribe
 * Saves a new subscriber's email to an S3 object.
 */
app.post('/subscribe', async (req, res) => {
    const { email } = req.body;

    if (!email || !email.includes('@')) {
        return res.status(400).json({ message: 'Invalid email address provided.' });
    }

    // We'll save each email as a separate object for simplicity.
    // Your Lambda can then list all objects in the 'subscriptions/' path.
    // Using the email as the key (after sanitizing) makes it easy to avoid duplicates.
    const sanitizedEmail = email.replace(/[^a-zA-Z0-9-_.@]/g, '');
    const s3Key = `${SUBSCRIPTIONS_PATH}${sanitizedEmail}`;

    console.log(`Saving subscription to s3://${S3_BUCKET_NAME}/${s3Key}`);

    const putObjectParams = {
        Bucket: S3_BUCKET_NAME,
        Key: s3Key,
        Body: email, // The content of the file will just be the email address
        ContentType: 'text/plain',
    };

    try {
        // Create and send the PutObjectCommand
        const command = new PutObjectCommand(putObjectParams);
        await s3Client.send(command);

        res.status(201).json({ message: 'Subscription successful!' });

    } catch (error) {
        console.error('S3 PutObject error:', error);
        res.status(500).json({ message: 'Failed to save subscription.', error: error.message });
    }
});

// --- Start Server ---
app.listen(PORT, () => {
    console.log(`Weather app backend listening on http://localhost:${PORT}`);
    console.log('---');
    console.log(`Ensure S3_BUCKET_NAME is set. Current value: ${S3_BUCKET_NAME}`);
    console.log(`Ensure AWS_REGION is set. Current value: ${S3_REGION}`);
    console.log('Ensure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set in your environment.');
    console.log('---');
});
