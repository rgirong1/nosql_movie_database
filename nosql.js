const MongoClient = require('mongodb').MongoClient;
var url = "mongodb://localhost:27017/";

var fs = require('fs');
var filename = 'movies.dat';
//var filedata = [];

const prompt = require('prompt-sync')({sigint: true});

//Log in to user profile, requires ID
var userlogin = prompt("Enter your user ID: ");
var choice = 0;
var fdstr = [];

function choosePrompt() {
    console.log("Nosql Interface (Choose what you want to do): \n 1. Read Rating Data from File(s) \n 2. Add new rating \n 3. Add new movie entry \n 4. Add new user account \n 5. Display movies in database \n 6. Display all user ratings \n 7. Show Aggregate Ratings \n 8. Get Movies Recommended by Genre \n 9. Find Users Like You \n 10. Exit \n\n");
    var promptres = prompt("Enter Choice:");
    return promptres;
    //console.clear();
}

// Ask for filename from user
function readFile() {
    return prompt("Enter file to read: ");
}

// Read file with data from movietweetings dataset. Each file requires different logic in order to properly convert it into a json object.
async function getFileData(client) {
    filename = readFile();
    var filedata = fs.readFileSync(filename);
    var ratingdata = fs.readFileSync("ratings.dat");
    var moviedata = fs.readFileSync("movies.dat");
    fdcollect = "Movies";
    if (filename == "movies.dat") {
        fdstr = parseMovieDataSet(client, filedata, ratingdata);
        console.log(fdstr);
        
        const res = await client.db("MovieRatings").collection("Movies").insertMany(fdstr);
        console.log(`${res.insertedCount} movie added with id:`);
        console.log(res.insertedID);
        //console.log(mfstr);
    }
    if (filename == "users.dat") {
        fdstr = parseUserDataSet(client, filedata, ratingdata, moviedata);

        console.log(fdstr);
        
        const res = await client.db("MovieRatings").collection("Users").insertMany(fdstr);
        console.log(`${res.insertedCount} users added with id:`);
        console.log(res.insertedID);
        //console.log(ufstr);
    }
    if (filename == "ratings.dat") {
        fdstr = parseRatingDataSet(client, filedata);

        console.log(fdstr);
        
        const res = await client.db("MovieRatings").collection("Ratings").insertMany(fdstr);
        console.log(`${res.insertedCount} ratings added with id:`);
        console.log(res.insertedID);
        //console.log(rfstr);
    }
    
}

///Takes the data from the movie dataset and converts it into the proper object format
function parseMovieDataSet(client, data, ratingdata) {
    
    //Split Data into array of strings
    var movstr = data.toString().split(/\r?\n/);
    var ratstr = ratingdata.toString().split(/\r?\n/);

    //Convert movie data from online dataset to proper mognodb format
    //var mformatstr;
    var mformatobj = [];
    var mobj = [];
    for(var i = 0; i <= 100; i++) {
        var mformatstr = '[{ "id":  "' + movstr[i].replace('::','", "name": "').replace(' (','", "year": "').replace(')','').replace('::','", "genre": ["').replace('|','", "') + '"]' /*+ '"ratings": []'*/ + '}]';
        mobj = JSON.parse(mformatstr);
        mformatobj.push(mobj[0]);
        mformatobj[i].ratings = [];
    }
    
    //Convert ratings data from online dataset to proper mognodb format
    var rformatobj = [];
    var robj = [];
    for(var i = 0; i <= 1000; i++) {
        var rformatstr = '[{ "userID":  "' + ratstr[i].replace('::','", "movieID": "').replace('::','", "rating": "').replace('::','", "ratingtime": "') + '" }]';
        robj = JSON.parse(rformatstr);
        rformatobj.push(robj[0]);
    }
    for (var i = 0; i < rformatobj.length; i++){
        rformatobj[i].rating = parseInt(rformatobj[i].rating);
    }
    
    //Add data from ratings dataset to the movie dataset
    for (var i = 0; i < mformatobj.length; i++){
        for (var j = 0; j < rformatobj.length; j++) {
            if (rformatobj[j].movieID == mformatobj[i].id) {
                mformatobj[i].ratings.push({user: rformatobj[j].userID, rating: rformatobj[j].rating, ratingtime: rformatobj[j].ratingtime});
            }
        }
    }
    
    return mformatobj;
}

///Takes the data from the user dataset and converts it into the proper object format
function parseUserDataSet(client, data, ratingdata, moviedata) {
    
    //Split Data into array of strings
    var usrstr = data.toString().split(/\r?\n/);
    var ratstr = ratingdata.toString().split(/\r?\n/);
    var movstr = moviedata.toString().split(/\r?\n/);

    //Convert user data from online dataset to proper mognodb format
    var uformatobj = [];
    var uobj = [];
    for(var i = 0; i <= 10; i++) {
        var uformatstr = '[{ "id":  "' + usrstr[i].replace('::','", "twitterID": "') + '" }]';
        uobj = JSON.parse(uformatstr);
        uformatobj.push(uobj[0]);
        uformatobj[i].ratings = [];
    }
    
    //Convert ratings data from online dataset to proper mognodb format
    var rformatobj = [];
    var robj = [];
    for(var i = 0; i <= 1000; i++) {
        var rformatstr = '[{ "userID":  "' + ratstr[i].replace('::','", "movieID": "').replace('::','", "rating": "').replace('::','", "ratingtime": "') + '" }]';
        robj = JSON.parse(rformatstr);
        rformatobj.push(robj[0]);
    }
    for (var i = 0; i < rformatobj.length; i++){
        rformatobj[i].rating = parseInt(rformatobj[i].rating);
    }
    
    //Convert movie data from online dataset to proper mognodb format
    //var mformatstr;
    var mformatobj = [];
    var mobj = [];
    for(var i = 0; i <= 100; i++) {
        var mformatstr = '[{ "id":  "' + movstr[i].replace('::','", "name": "').replace(' (','", "year": "').replace(')','').replace('::','", "genre": ["').replace('|','", "') + '"]' + '}]';
        mobj = JSON.parse(mformatstr);
        mformatobj.push(mobj[0]);
    }
    
    for (var i = 0; i < mformatobj.length; i++){
        for (var j = 0; j < rformatobj.length; j++) {
            if (rformatobj[j].movieID == mformatobj[i].id) {
                rformatobj[j].movieID = mformatobj[i].id;
                console.log(rformatobj[j]);
            }
        }
    }
    
    //Add data from ratings dataset to the movie dataset
    for (var i = 0; i < uformatobj.length; i++){
        for (var j = 0; j < rformatobj.length; j++) {
            if (rformatobj[j].userID == uformatobj[i].id) {
                uformatobj[i].ratings.push({movie: rformatobj[j].movieID, rating: rformatobj[j].rating, ratingtime: rformatobj[j].ratingtime});
                console.log(uformatobj[i].ratings);
            }
        }
    }
    
    return uformatobj;
}

///Takes the data from the ratings dataset and converts it into the proper object format
function parseRatingDataSet(client, data) {
    
    //Split Data into array of strings
    var ratstr = data.toString().split(/\r?\n/);

    //Convert ratings data from online dataset to proper mognodb format
    var rformatobj = [];
    var robj = [];
    for(var i = 0; i <= 10; i++) {
        var rformatstr = '[{ "userID":  "' + ratstr[i].replace('::','", "movieID": "').replace('::','", "rating": "').replace('::','", "ratingtime": "') + '" }]';
        robj = JSON.parse(rformatstr);
        rformatobj.push(robj[0]);
    }
    for (var i = 0; i < rformatobj.length; i++){
        rformatobj[i].rating = parseInt(rformatobj[i].rating);
    }
    
    return rformatobj;
}

//Ask User for movie title and rating in order to add it to the database. If the movie does not currently exist in the database it will ask the user for additional information to make an entry for this movie.
async function addRating(client) {
    var movietitle = prompt("Enter Name of Movie: ");
    var movierating = parseInt(prompt("Enter Rating: "));

    var d = new Date();
    var ratingobjmov = {user: userlogin, rating: movierating, ratingtime: d.getTime()};
    var ratingobjusr = {movie: movietitle, rating: movierating, ratingtime: d.getTime()};
    //var ratingobj = {userID: userlogin, movieID: movietitle, rating: movierating, ratingtime: d.getTime()};
    
    const res = await client.db("MovieRatings").collection("Movies").updateOne(
        { name: movietitle },
        { $addToSet: {ratings: ratingobjmov}}
        );
    
    await client.db("MovieRatings").collection("Users").updateOne(
        { id: userlogin },
        { $addToSet: {ratings: ratingobjusr}}
    );
    
    //const res = await client.db("MovieRatings").collection("Ratings").insertOne(ratingobj);
    
    console.log(`${res.insertedCount} rating for ${movietitle} added by user with id ${userlogin}`);
    
    return [movietitle, movierating];
}

//Clear all 3 mongodb collections of all of their data
async function clearCollections(client) {
    
    await client.db("MovieRatings").collection("Movies").deleteMany();
    
    await client.db("MovieRatings").collection("Users").deleteMany();
    
    await client.db("MovieRatings").collection("Ratings").deleteMany();
}

//Ask User for movie title in order to remove the rating for that movie from the database.
async function removeRating(client) {
    var movietitle = prompt("Enter Name of Movie: ");
    
    const res = await client.db("MovieRatings").collection("Movies").deleteOne(
        { name: movietitle }
        );
    
    await client.db("MovieRatings").collection("Users").deleteOne(
        { id: userlogin },
        { $addToSet: {ratings: ratingobjusr}}
    );
    
    console.log(`${res.insertedCount} rating added with id:`);
    console.log(res.insertedID);
    
    return [movietitle, movierating];
}

//Ask user for information on a movie entry in order to add it to the database.
function newMovieEntry(title) {
    console.log("That movie doesn't have an entry in the database, please enter information on the movie in order to add it to the database. \n\n")
    var movieyear = prompt("Enter Movie Year: ");
    var moviegenres = prompt("Enter the Movie Genre's: ");
    return [movieyear, moviegenres];
}

//Directly add a movie entry to the database, with empty ratings field
async function addMovie(client) {
    var movietitle = prompt("Enter Name of Movie: ");
    var movieyear = prompt("Enter Movie Year: ");
    var moviegenres = prompt("Enter the Movie Genre's: ");
    
    var movieobj = {name: movietitle, year: movieyear, genre: moviegenres.split(","), ratings: []};
        
    const res = await client.db("MovieRatings").collection("Movies").insertOne(movieobj);
    console.log(`${res.insertedCount} movie added!`);
    //console.log(res.insertedID);
}

//Directly add a user entry to the database, with empty ratings field
async function addUser(client) {
    var twitterhandle = prompt("Enter twitter id: ");
    
    var userobj = {id: userlogin, twitterID: twitterhandle, ratings: []};
        
    const res = await client.db("MovieRatings").collection("Users").insertOne(userobj);
    
    console.log(res);
}

//Displays all movies that are currently in the database, including information such as the year the movie came out and its genres.
async function getMovies(client) {
    
    //Displays all movies found in the database
    const cursor = await client.db("MovieRatings").collection("Movies").find({
        
    })
    .sort({name: 1})
    .limit(1000);
    
    //Return data from mongodb, in the form of an array
    const res = await cursor.toArray();
    
    //Loop through the result array, and outputs data into the terminal
    if (res.length > 0) {
        console.log(`Movies found in database:`);
        res.forEach((res, i) => { 
            
            console.log();
            console.log(`${i + 1}. name: ${res.name} (${res.year})`);
            console.log(`  _id: ${res._id}`);
            console.log(`  genre's: ${res.genre}`);
        });
    } else {
        console.log('No Movies Found');
    }
}

//Insert a user id to get all of the ratings that the user has made. Displays the movie and rating given to it by the user.
async function getUserRatings(client) {
    
    var userid = prompt("Enter User ID: ");
    
    const cursor = await client.db("MovieRatings").collection("Users").find(
        {id: userid}
    )
    .limit(1);
    
    //Return data from mongodb, in the form of an array
    const res = await cursor.toArray();
    
    //Loop through the result array, and outputs data into the terminal
    if (res.length > 0) {
        console.log(`Ratings from user ${res.twitterID}`);
        res.forEach((res, i) => {
            
            for(var i = 0; i < res.ratings.length; i++) {
                console.log();
                console.log(`${i + 1}. ${res.ratings[i].movie} rating: ${res.ratings[i].rating}/10`);
                var hd = new Date(res.ratings[i].ratingtime).toLocaleDateString("en-US")
                console.log(`  rating date: ${hd}`);
            }
        });
        //console.log(res);
    } else {
        console.log(`No Movie Ratings Found from user ${userid}`);
    }
    
}

//Searches through the movie collection and finds all ratings for each movie. Then finds the average value of all the ratings a movie has recieved and displays that value as the consensus movie rating. Movies are displayed by highest rating.
async function getAggregateRatings(client) {
    
    const aggCursor = await client.db("MovieRatings").collection("Movies").aggregate([
        {"$unwind": "$ratings"},
        {$group: {
            _id: "$name",
            avgRating: {$avg: "$ratings.rating"},
            //ratingtime: "$ratingtime"
        }},
        {$sort: {avgRating: -1}}
    ]);
    
    //Return data from mongodb, in the form of an array
    const res = await aggCursor.toArray();
    
    //Loop through the result array, and outputs data into the terminal
    if (res.length > 0) {
        console.log(`Ratings from database:`);
        res.forEach((res, i) => {
            //date = new Date(res.)       
            
            console.log();
            console.log(`${i + 1}. ${res._id} rating: ${res.avgRating}`);
        });
    } else {
        console.log('No Ratings Found');
    }
}

// Recieves a genre from the user. Depending on the genre that is searched the query will find movies that fit the search that have a large average rating.
async function getRecommendedMovieByGenre(client) {
    var genreSearch = prompt("Enter Genre to get Recommendations from: ");
    
    const aggCursor = await client.db("MovieRatings").collection("Movies").aggregate([
        {"$unwind": "$genre"},
        {"$match": {genre: genreSearch}},
        {"$unwind": "$ratings"},
        {$group: {
            _id: "$name",
            avgRating: {$avg: "$ratings.rating"}
        }},
        {$sort: {avgRating: -1}}
        
    ]);
    
    //Return data from mongodb, in the form of an array
    const res = await aggCursor.toArray();
    
    //Loop through the result array, and outputs data into the terminal
    if (res.length > 0) {
        console.log(`Movies in the ${genreSearch} genre you might like:`);
        res.forEach((res, i) => {
            //date = new Date(res.)       
            
            console.log();
            console.log(`${i + 1}. ${res._id} rating: ${res.avgRating}`);
        });
    } else {
        console.log('No Movies Found');
    }
}

// Ask the user for a movie that they enjoy. Searches through every other user in the database to see if they have given that movie a rating of at least a 6. If they have then it will output their id and their twitterID. 
async function findUsersWithSimilarTaste(client) {
    var movieSearch = prompt("Enter movie you like: ");
    
    const aggCursor = await client.db("MovieRatings").collection("Users").aggregate([
        {"$unwind": "$ratings"},
        {"$match": {"ratings.rating": {$gt: 5}}},
        {"$match": {"ratings.movie": movieSearch}}
    ]);
    
    //Return data from mongodb, in the form of an array
    const res = await aggCursor.toArray();
    
    //Loop through the result array, and outputs data into the terminal
    if (res.length > 0) {
        console.log(`User's that like ${movieSearch}:`);
        res.forEach((res, i) => {
            //date = new Date(res.)       
            
            console.log(`user id: ${res.id}, twitter id: ${res.twitterID}`);
            //console.log(`${i + 1}. ${res._id} rating: ${res.avgRating}`);
        });
    } else {
        console.log('No Users Found');
    }
}

async function userGUI(client) {
    choice = choosePrompt();

    var exitval = 10;
    while (choice != exitval) {
        switch(choice) {
            case "1":
                await getFileData(client);
                break;
            case "2":
                var ratingres = await addRating(client);
                console.log(ratingres[0] + ratingres[1]);
                break;
            case "3":
                await addMovie(client);
                break;
            case "4":
                await addUser(client);
                break;
            case "5":
                await getMovies(client);
                break;
            case "6":
                await getUserRatings(client);
                break;
            case "7":
                await getAggregateRatings(client);
                break;
            case "8":
                await getRecommendedMovieByGenre(client);
                break;
            case "9":
                await findUsersWithSimilarTaste(client);
                //await getRecommendedMovieByPreference(client);
                break;
            case "10":
                console.log("Exit");
                break;
            case "11":
                await clearCollections(client);
                break;
        }
        if (choice != exitval) {
            choice = choosePrompt();
        }
    }
}

async function listDatabases(client) {
    const databasesList = await client.db().admin().listDatabases();
    
    console.log("Databases: ");
    databasesList.databases.forEach(db => console.log(` - ${db.name}`));
    
}

async function main() {
    const uri = "mongodb+srv://rgirong1:rgirong1mongodbbing@cluster0.oelf3.mongodb.net/<dbname>?retryWrites=true&w=majority";
    const client = new MongoClient(uri, {useNewUrlParser: true, useUnifiedTopology: true});
    try {
        await client.connect();
        await userGUI(client);
        //await addMovie(client);
    } catch (err) {
        console.error(err);
    } finally {
        await client.close();
    }
}

main().catch(console.error);