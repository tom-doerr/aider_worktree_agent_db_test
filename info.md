docker compose setup that has a postgres db in a container
seperate container for each piece of functionality
one service renders and downloads the tagesschau.de website every 60 seconds and writes it to the postgresdb
another gets the data from the postgresdb, parses it into individual new sarticles and writes them back to the db
another service uses dspy to categories them and write that data to the db
another service gets the newest articles and then using a dspy pipeline generates a commentary article based on the articles in the db and writes the commentary article back to the db
