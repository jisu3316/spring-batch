create table person (
    id bigint primary key auto_increment,
    name varchar(255),
    age varchar(255),
    address varchar(255)
);

insert into person(name, age, address) VALUES('김지수','27','안산');
insert into person(name, age, address) VALUES('홍길동','30','서울');
insert into person(name, age, address) VALUES('강감찬','25','인천');

