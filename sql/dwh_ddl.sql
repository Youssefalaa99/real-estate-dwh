CREATE TABLE "fct_sales" (
  "sale_id" integer PRIMARY KEY,
  "lead_id" integer NOT NULL,
  "date_of_reservation_id" integer,
  "date_of_contraction_id" integer,
  "property_type_id" integer,
  "compound_id" integer,
  "unit_value" integer,
  "expected_value" integer,
  "actual_value" integer,
  "years_of_payment" numeric(4,1),
  "sale_category" varchar
);

CREATE TABLE "fct_leads" (
  "lead_id" integer PRIMARY KEY,
  "customer_id" integer,
  "user_id" integer,
  "compound_id" integer,
  "lead_type_id" integer,
  "budget" integer,
  "is_buyer" boolean,
  "is_seller" boolean,
  "is_commercial" boolean,
  "is_merged" boolean,
  "do_not_call" boolean,
  "meeting_flag" int,
  "lead_source" varchar,
  "campaign" varchar,
  "method_of_contact" varchar,
  "status_name" varchar,
  "time_to_call" varchar,
  "created_at" timestamp,
  "updated_at" timestamp,
  "date_of_last_request" timestamp,
  "date_of_last_contact" timestamp
);

CREATE TABLE "dim_user" (
  "user_id" integer PRIMARY KEY,
  "name" varchar,
  "department" varchar
);

CREATE TABLE "dim_customer" (
  "customer_id" integer PRIMARY KEY,
  "name" varchar,
  "address" varchar,
  "phone_number" varchar,
  "customer_type" varchar
);

CREATE TABLE "dim_developer" (
  "developer_id" integer PRIMARY KEY,
  "name" varchar
);

CREATE TABLE "dim_compound" (
  "compound_id" integer PRIMARY KEY,
  "name" varchar,
  "area_id" integer,
  "developer_id" integer
);

CREATE TABLE "dim_area" (
  "area_id" integer PRIMARY KEY,
  "location" varchar,
  "region" varchar
);

CREATE TABLE "dim_property_type" (
  "property_type_id" integer PRIMARY KEY,
  "property_type" varchar
);

CREATE TABLE "dim_date" (
  "date_id" integer PRIMARY KEY,
  "full_date" date,
  "day" integer,
  "month" int,
  "year" int,
  "quarter" int,
  "day_of_week" int
);

CREATE TABLE "dim_lead_type" (
  "lead_type_id" integer PRIMARY KEY,
  "lead_type" varchar,
  "description" varchar
);

ALTER TABLE "fct_sales" ADD FOREIGN KEY ("lead_id") REFERENCES "fct_leads" ("lead_id");

ALTER TABLE "fct_sales" ADD FOREIGN KEY ("date_of_reservation_id") REFERENCES "dim_date" ("date_id");

ALTER TABLE "fct_sales" ADD FOREIGN KEY ("date_of_contraction_id") REFERENCES "dim_date" ("date_id");

ALTER TABLE "fct_sales" ADD FOREIGN KEY ("property_type_id") REFERENCES "dim_property_type" ("property_type_id");

ALTER TABLE "fct_sales" ADD FOREIGN KEY ("compound_id") REFERENCES "dim_compound" ("compound_id");

ALTER TABLE "fct_leads" ADD FOREIGN KEY ("customer_id") REFERENCES "dim_customer" ("customer_id");

ALTER TABLE "fct_leads" ADD FOREIGN KEY ("user_id") REFERENCES "dim_user" ("user_id");

ALTER TABLE "fct_leads" ADD FOREIGN KEY ("compound_id") REFERENCES "dim_compound" ("compound_id");

ALTER TABLE "fct_leads" ADD FOREIGN KEY ("lead_type_id") REFERENCES "dim_lead_type" ("lead_type_id");

ALTER TABLE "dim_compound" ADD FOREIGN KEY ("area_id") REFERENCES "dim_area" ("area_id");

ALTER TABLE "dim_compound" ADD FOREIGN KEY ("developer_id") REFERENCES "dim_developer" ("developer_id");

