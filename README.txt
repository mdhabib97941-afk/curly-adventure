=========================================
ALPHA FLOW PRO - FULL STACK DEPLOYMENT
=========================================

This is a ready-to-run Node.js application that includes both your original HTML interface and a new backend server that collects historical order book data (bids and asks) every hour, day, and week.

--- NO DATABASE SETUP REQUIRED ---
This application uses SQLite, which is a zero-configuration database. It will automatically create a file called `database.sqlite` when you run the app. There are no database credentials, usernames, or WordPress-style installers needed!

--- HOW TO DEPLOY ON YOUR HOSTING ---

1. Upload the contents of this ZIP file to your hosting provider.
2. In your hosting's cPanel or Node.js App Manager, set the application startup file to:
   server.js
3. Click the "Run NPM Install" button in your hosting panel (this will install Express and SQLite automatically).
4. Start/Restart the App.

Your site will now be live! If you visit the page, you will see the new "Historical Order Book Snapshots" section at the bottom, which will automatically update as the backend collects data over time.

--- NOTE ON DATA COLLECTION ---
The backend will collect data continuously as long as the server is running. A sample snapshot is taken 5 seconds after you start the server so you can test that the historical tables are working immediately!
