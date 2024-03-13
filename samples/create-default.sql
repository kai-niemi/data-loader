create table customer
(
    id         int8         not null default unordered_unique_rowid(),
    email      varchar(128),
    name       varchar(128),
    password   varchar(128) not null,
    address1   varchar(255),
    address2   varchar(255),
    postcode   varchar(16),
    city       varchar(255),
    country    varchar(36),
    updated_at timestamptz  not null default clock_timestamp(),

    primary key (id)
);

create table purchase_order
(
    id             int8 not null default unordered_unique_rowid(),
    bill_address1  varchar(255),
    bill_address2  varchar(255),
    bill_city      varchar(255),
    bill_country   varchar(16),
    bill_postcode  varchar(16),
    bill_to_name   varchar(255),
    deliv_address1 varchar(255),
    deliv_address2 varchar(255),
    deliv_city     varchar(255),
    deliv_country  varchar(16),
    deliv_postcode varchar(16),
    deliv_to_name  varchar(255),
    date_placed    date not null,
    status         varchar(64),
    amount         numeric(19, 2),
    currency       varchar(255),
    version        int4,
    customer_id    int8 not null,

    primary key (id)
);

create table purchase_order_item
(
    order_id   int8   not null,
    product_id int8,
    quantity   int4   not null,
    status     varchar(64),
    amount     numeric(19, 2),
    currency   varchar(255),
    weight_kg  float8 not null,
    item_pos   int4   not null,

    primary key (order_id, item_pos)
);

alter table if exists customer
    add constraint UK_8cqc86mekfecc5kjcwakm36nd unique (email);

alter table if exists purchase_order
    add constraint FK_esy3n2gc3fa0s3trrk3tvyv9a
        foreign key (customer_id)
            references customer;

alter table if exists purchase_order_item
    add constraint FK_t3r2219w9rbs121eas5yf025m
        foreign key (order_id)
            references purchase_order;


