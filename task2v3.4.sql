create table header
(
    contract_number char(10) not null,
    contract_date   date     not null,
    primary key (contract_number)
);

create table sales
(
    salesman_number char(8)     not null,
    salesman_name   varchar(50) not null,
    mobile_phone    char(11)    not null,
    age             int         not null,
    gender          varchar(6)  not null,
    primary key (salesman_number)
);

create table product_model
(
    product_model varchar(100) not null,
    unit_price    int          not null,
    primary key (product_model)
);

create table product_class
(
    product_code varchar(7)   not null,
    product_name varchar(100) not null,
    primary key (product_code)
);

create table supply
(
    supply_center varchar(30) not null,
    director      varchar(30) not null,
    primary key (supply_center)
);

create table client
(
    client_enterprise varchar(100) not null,
    city              varchar(20),
    industry          varchar(50)  not null,
    primary key (client_enterprise)
);

create table country
(
    country varchar(50) not null,
    primary key (country)
);

create table "order"
(
    contract_number         char(10)     not null,
    product_model           varchar(100) not null,
    salesman_number         char(8)      not null,
    quantity                int          not null,
    estimated_delivery_date date         not null,
    lodgement_date          date,
    primary key (contract_number, product_model),
    foreign key (contract_number) references header,
    foreign key (product_model) references product_model,
    foreign key (salesman_number) references sales
);

create table model_class
(
    product_code  varchar(7)   not null,
    product_model varchar(100) not null,
    primary key (product_model),
    foreign key (product_code) references product_class,
    foreign key (product_model) references product_model
);

create table contract
(
    contract_number   char(10)     not null,
    supply_center     varchar(30)  not null,
    client_enterprise varchar(100) not null,
    primary key (contract_number),
    foreign key (contract_number) references header,
    foreign key (client_enterprise) references client,
    foreign key (supply_center) references supply
);

create table client_country
(
    client_enterprise varchar(100) not null,
    country           varchar(50)  not null,
    primary key (client_enterprise, country),
    foreign key (client_enterprise) references client,
    foreign key (country) references country
);