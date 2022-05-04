CREATE  KEYSPACE hw2_omelkina WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE hw2_omelkina;

CREATE TABLE favorite_songs (id int, author text, song_name text,release_year int, PRIMARY KEY (id));
CREATE TABLE favorite_movies (id int, name text, producer text,release_year int, PRIMARY KEY (id));