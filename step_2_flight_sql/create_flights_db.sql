create database flights;
create role flights_user with login password 'testpass';
grant flights_user to postgres;
alter database flights owner to flights_user;

