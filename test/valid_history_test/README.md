# Common data management patterns: `jaffle_shop`

## What is this repo?
This is a fork of the dbt `jaffle_shop` project we (Teradata) use to illustrate some of our key advocated positions on data management using dbt.

This project provides a base for demonstration and collaboration on common data management patterns for analytics, and doesn't intend to be a dbt tutorial (check the original jaffle shop project for this), or a production-grade package (the reality of your orgnisation is likely more complex than a sandwich shop).

## Why should you care?
Because data management gets rapidly complex as you scale up with data products, teams and consumers, and too often this results in your organization's trust in its data decreasing precisely as its demand increases...

At Teradata, we build solutions that scale from start. We believe that shared data architecture standards underpin how well data can be used, re-used, and collaborated upon. 

The data management patterns illustrated herein embody some of our strongest advocated positions, and represent some of the standards to be set for a trusted data management practice.

Use this if you want to:
- Demo and discuss common data management patterns with your colleagues and partners.
- Evaluate dbt and Teradata fitness for your use case and start developing your own framework.
- Get a free Teradata environment running and loaded with a demo dataset.

## What is in there?

This project is a fork from dbt's original "jaffle shop" project, as we intend to leverage the community's familiarity with this project, we try to implement as little change as possible to its model, inputs and outputs (ie. "what" it does), but propose some alternative ways to achieve the same goals using what we believe are time and scale tested data management patterns.

**Project structure:**

We illustrate [how we structure a project](#project-structure) according to Teradata's reference information architecture. 

![](./etc/connected-data-foundation.jpg)

**Patterns:**

Each pattern is illustrated with a simplified code boilerplate, additionally we propose macros automating (most of) the model generation for common patterns.

- [Staging](#pattern-staging)
- [Source Image](#pattern-source-image)
- [Surrogate key management](#pattern-surrogate-key-management)
- [Light integration](#pattern-light-integration)
- [History management](#pattern-history-management)
- [Access layer](#pattern-access-layer)

## What is the jaffle shop project

`jaffle_shop` is a fictional ecommerce store. This dbt project transforms raw
data from an app database into a customers and orders model ready for analytics.

The raw data from the app consists of customers, orders, and payments, with the following entity-relationship diagram:
![](./etc/jaffle_shop_erd.png)

## Running this project

### Get a free Teradata environment
Register to https://clearscape.teradata.com/ to get a free Teradata environment.

Create an new environment.

### Create a dbt connection profile

Update your dbt profile with the Teradata connection, by adding the following to your profiles.yml (default `~/.dbt/profiles.yml`)

```
jaffle_shop_teradata:
  outputs:
    dev:
      type: teradata
      host: <HOST NAME HERE>
      user: demo_user
      password: <PASSWORD HERE>
      logmech: TD2
      schema: demo_user
      tmode: ANSI
      threads: 1
      timeout_seconds: 300
      priority: interactive
      retries: 1
  target: dev
```

### Get this project, dbt and dependencies
**TL;DR (Windows)**
```
git clone https://github.com/Teradata/jaffle_shop_teradata.git
cd jaffle_shop_teradata
python -m venv venv
venv\Scripts\activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
venv\Scripts\activate
```

### Run this project
```
dbt seed
dbt run
dbt docs generate
dbt docs serve
```

### Explore this project
We recommend using Visual Studio Code with the [Power User for dbt](https://marketplace.visualstudio.com/items?itemName=innoverio.vscode-dbt-power-user) for the best interactive experience with dbt and Teradata.
Don't forget to make sure that you select the correct Python interpreter (Command Pallette > "Python: Select Interpreter": It should be the virtual environment created in first step).

![](./etc/vscode.jpg)

## Proposed Implementation

### Project structure

At Teradata, we advocate that data is preserved in a common foundation and organized in zones. Each zone is optimised for different activities:
- Raw: receiving and preserving source data in its original form.
- Discoverable: Organizing data for discovery and rapid ideation.
- Reusable: Data optimized for reuse across multiple applications.
- Consumable: Data optimized for consumption by downstream applications.

Data products (and corresponding dbt models) live in each zone, data flows overall from raw to consumable without necessarily having an intermediate product in every zone or ending in a "consumable" product (most data is only accessed for discovery and never used in a final business product). 
This data pipelines decomposition offers the opportunity to standardize the logic performed in each step and increases the opportunity for data product reuse in every zone.

![](./etc/connected-data-foundation-zones.jpg)

The model directory structure aligns to the zones:
- [./models/consumption](./models/consumption). Contains logic for managing access schemas for downstream applications.
- [./models/core](./models/core): Contains logic defining common summaries, calculations or references management.
- [./models/discovery](./models/discovery): Contains models that minimally prepare, integrate and secure source data, just enough so it can be discovered and used for ideation.
- [./models/meta](./models/meta): Management of metadata such as keys, business and technical metadata.
- [./models/raw](./models/raw): Logic to manage incoming data and its history in its raw form.


### Pattern: Staging
**Why**: The staging pattern provides a standard way to handle and access incoming data wether it is made available on an external storage or loaded onto the platform by a tool. This layer provides isolation between the load utilities and the downstream consumption processes.

**What needs to be done**: 
1. Point to incoming data (from an external storage or loaded by a tool),
2. Add standard control columns for this layer (eg. when was the record received),
3. If necesary, overrides physical data types (eg. if not properly preserved by the E/L tools).

**How**: Create a model for each sourced entity that performs the above logic.
- Manage source object locking: if the incoming data is directly written onto the managed storage, ensure explicit access lock definition to avoid locking the write process.
- Select all incoming columns, filter out technical ones added by the transport mechanism if necessary, adjust data types if absolutely necessary.
- The minimum viable standard control column should be a timestamp indicating when this record landed on the platform (ie. for materialisations the current database timestamp or dbt's `run_started_at` , for views over external storage a derivation from the source object path or name if available or the current database timestamp as default). To ensure consistency, we recommend defining this timestamp name and definition as a project variable.
- When sourcing from external storage: use a foreign table object, specify the source metadata using [dbt sources](https://docs.getdbt.com/docs/build/sources) for lineage. Ensure that the foreign table object is created, [we provide a macro to manage foreign tables](./macros/framework/foreign_tables.sql) that you can place in your model's pre-hook statement.
- Materialization choice: materialization here enables to accumulate incoming records in a staging table and "flush" the incoming one independently of the downstream processes. If you go with a source image pattern (see next) this is useful (eg. allows indexing, separation of concerns in orchestration) but not strictly necessary. Since we are not concerned with data rules at this point, we cannot define a primary key, use the `append` materialization strategy.

**Examples**
- Defining a standard control column name for record landing timestamp: see `last_update_ts` in [./dbt_project.yml](./dbt_project.yml)
- Staging from an external object storage using a foreign table: [./models/raw/staging/stg_market_customers.sql](./models/raw/staging/stg_market_customers.sql)
- A simple example staging from data landed on the managed storage [./models/raw/staging/stg_payments.sql](./models/raw/staging/stg_payments.sql) (in this case a seed). 

### Pattern: Source Image
**Why**: The source data is the persistent "gold", every derived object in every analytical warehouse, mart and model is a derived product of it, and can and will eventually be rebuilt from it. Inconsistencies between analytic datasets are resolved at the source system. However the costs and timelines of accessing source systems data are often prohibitive and the burden of rebuilding such picture from a system's changelog needs to be minimized.

Building a common common source image for your analytic projects is by far the greatest opportunity for reuse, standardization and automation or work.

**What needs to be done**: Build a complete, identical, and ideally historical picture of the source system state. We "mirror" each entity gathering the incoming data from staging and consolidating it in a persistent table. The results must be consistent regardless the source delivery mechanism (full extracts, change data capture), the resulting object needs to be an exact representation of the corresponding source entity (eg. no "data cleansing" logic, same or loser data types: "touch it, take it") and we preserve and expose history as much as possible and reasonable.

**How**:
Create a model for each mirrored entity that:
1. "Mirror" the structure of the source (staging) table, filtering out technical columns if needed.
2. Compute delta logic.
3. Persist data accounting for incremental and history management. 

**Note on history management:** We implement a [*valid time*](https://en.wikipedia.org/wiki/Valid_time) history that reflects the validity of information as observed in the source system. This requires that we identify and map the best proxy for a record change date/time *in the source system*. This is often provided by the change data capture mechanism, can be approximated form the transport metadata (eg. message queue), and in last resort be defaulted to the ingestion timestamp captured by the staging model.
We use the [*valid_history* incremental strategy provided by the Teradata adapter](https://en.wikipedia.org/wiki/Valid_time), this implementation handles the complexities of transforming a series of change events into a timeline, inserting backdated data (eg. processing delayed messages) without having to rebuild everything to date, and merging consecutive and identical time slices. 

As there is no business logic implemented and the model logic is a function the structure and nature of the source feed, this can be fully packaged in a macro. 
The macro *build_source_image* provided in [./macros/framework/source_image.sql](./macros/framework/source_image.sql) inherits the models configuration parameters to generate the source image logic, with the exception of the delta capture mechanism that is left to the discretion of the model developer.

**Examples**
- [./models/raw/source-image/sim_payments.sql](./models/raw/source-image/sim_payments.sql) Simple "manual" source image build. Delta capture, append only, no history.
- [./models/raw/source-image/sim_customers.sql](./models/raw/source-image/sim_customers.sql) Historical source image build, using the macro and generic delta identification. Just change the source table name and config parameters to create a new source image model!

### Pattern: Surrogate key management
**Why**: Keys identify the members of major entities in your model (customers, products & services, accounts... ). Keys are the first data domains to standardize across products if we want them to inter-operate (read: join two datasets from two different teams on the "customer key" because we trust that the same value represents the same customer).
While systems of record typically provide their own keys, they are by definition bound to this system context and will eventually fall short for the purpose of analytics.

**Note on data privacy**: A useful byproduct of key surrogation is data protection: because natural identifiers for key entities are often Personally Identifiable Information data elements (think personal ID numbers, account numbers, email addresses, phone numbers, vehicles and devices identification...), a surrogate key process provides you with a "free" mechanism to tokenize this data, and secure the natural key values (PII) away where most your users can't access it. And reversibly, this provides you an easy mechanism to audit the PII content of your warehouses and ensure you comply to the regulations.

**What needs to be done**: Define a common service to centrally manage the surrogate key logic and enable their definition and usage at the model level. We advocate for: 
- Generating surrogate keys using meaningful natural identifiers (eg. an email address or a phone number for a user) in combination with a code identifying the identifier's domain (eg. 'email' or 'phone-number') to avoid collisions. 
- Retaining the natural key value and domain alongside their surrogate in a common key table that can be secured. Practically, we may define one table per major entity ("user", "account", "device").
- Provide a common mechanism to generate keys and lookup keys, so keys can be defined, generated and looked up in any model requiring them. This gives the data engineers the flexibility to define and use keys locally without having to rely on a centrally-managed key definition file.
- Performance matters as those are going to be you most common join paths, chose data types that work well for this purpose (eg. integers and not larger than it needs to be to handle another century worth of business growth) and a surrogation algorithm that outputs a population as dense as possible. 

**How**: The solution has two components, a central one composed of key tables and common key management macros, and one local to the model to define keys and invoke generation and lookup.
- Persistent key tables are defined by dedicated models placed in the `./meta/keys/` directory. The function of those models is only to generate the key table if it doesn't exist yet and populate the default key members (eg. the famous '-1/UNKNOWN' customer that currently subscribes to 23,431 services and generates over $420k of monthly revenue to your organization! ;))
- The macro [generate_surrogate_key](./macros/framework/surrogate_keys.sql) we provide as an example generate new surrogate keys from natural value and domain using a dense rank method. New key values are persisted in the key table. This generation is executed in a single transaction ensuring that concurrent processing does not result in duplicate key values.
- Surrogate keys can be defined with metadata in the models requiring them, this payload is passed to the key generation macro using in a pre-hook we provide a macro for conveniently generating the hook statement [generate_surrogate_key_hook](./macros/framework/surrogate_keys.sql). Note that we could use this mechanism to generate keys in a post-hook, right after the source data for it is landed, while this is more efficient, this requires to separate the key definition from the models using it, and centralising them to some extent.
- Surrogate keys can be looked up in the models requiring them with the help of the same metadata definition. 

**Examples**
- Key table definition model: [./models/meta/keys/key_customer.sql](./models/meta/keys/key_customer.sql)
- Surrogate key definition metadata: two "customer" keys identifying retail customers and families  (yes, we've got a reward programme for extended families and we haven't really found better than your last name to make this work!), we indicate that the natural family name needs to be masked (or maybe, we just deem this of analytic value but we don't want to expose names to our analysts or any AI/ML services...).


  ```
  surrogate_keys={
      'customer':{
        'source_table': 'sim_customers',
        'key_table': 'key_customer',
        'natural_key_cols': ['email'],
        'domain': 'retail',
      },
      'family':{
        'source_table': 'sim_customers',
        'key_table': 'key_customer',
        'natural_key_cols': ['last_name'],
        'domain': 'families',
        'mask': True
      }
    }
  ```

- A model calling the key generation hook and looking then adding the surrogates to its output: [./models/discovery/light-integration/lim_orders.sql](./models/discovery/light-integration/lim_orders.sql)


### Pattern: Light integration
**Why**: Time to value. Rapid innovation requies near unconstrained access to as much data as you can gather, regarless its current level of curration and documentation. Yet, we still need to ensure that data is secured (eg. sensitive data is obfuscated, access is controlled), and easy to navigate (eg. establish linkages to well known entities or semantics). The approach here is to overlay "light" integration and modeling over our source image.

**What needs to be done**:
1. Reflect the source image objects
2. Overlay the associated surrogate keys
3. Secure data at the row and/or column level (eg. removing, masking tokenizing, filtering... ) if the surrogation hasn't taken care of it yet.
4. Align column names and data types to standards.
 
**How**
This process can be mostly automated: if you don't need to align object names or data types, all you need to do is to define the surrogate keys. We provide the macro that does everything else: [build_light_integrated](./macros/framework/light_integration.sql).

**Examples**
- Simple, "manual", example: [./models/discovery/light-integration/lim_payments.sql](./models/discovery/light-integration/lim_payments.sql)
- Using the helper macro for complete model code generation from source table, with surrogate key overlay: [models/discovery/light-integration/lim_customers.sql](models/discovery/light-integration/lim_customers.sql)


### Pattern: Reuse layer
**Why**: Reduce complexity. Cost-effective operations at scale require data and logic reuse. Here we model common summaries, reference and "core" data to simplify reuse of new business logic (ie. what the source systems haven't already done for you) across application and domains.

**What DOES NOT need to be done**:
Only model and preserve here what is needed, ie. avoid simply replicating what is already provided by the source systems.

**What needs to be done**: Define the business logic that needs to be implemented to derive the new information and model it so it is easy to manage (ie. minimizing the number of writes and frequency your model (and consequently downstream) will have to run) and coherent with the current data model in this zone.

**How**: Define a model to implement your logic and extend the schema at this layer. We advocate for normalizing where possible in order to minimize the effort involved in updating the warehouse, its operational complexity and and separate ownership. In practice, this means manage information elements that are not directly dependent in separate entities (or break the back of transitive functional dependencies) as much as you can.

When deciding how to update the current schema, consider that you have three major data modeling patterns to chose from:
- Major entities: Defined by a primary key, and only the primary key, (ideally a reflection of a surrogate key defined earlier), holds directly dependent attributes. Eg. "customer".
- Attributive entities: Defined by a major entity and holds attributes describing that entity. Typically separate from the major entity in order to manage history, minimize the DAG complexity (eg. number of sources per model), or separate ownership (eg. Operations team owns "customer addresses", Sales owns "customer lifetime value"). Eg. "customer_address_history".
- Relationships: Defines by two or more major entities, records a relationship and attributes dependant on this relationship. May be historical (ie. valid during a certain time period). Eg. "customer_orders".

**Examples**
- Simple customer major entity, derived form the key table: [./models/core/customer.sql](./models/core/customer.sql)
- Attributive entity, using valid time history: [./models/core/customer_lifetime_value.sql](./models/core/customer_lifetime_value.sql)

### Pattern: Access layer
**Why**: Simplify access of end user applications and control user experience.

**What needs to be done**: Define the target access schema for the application (eg. snowflake schema, analytic dataset, key/value pairs), source data from the underlying layers, materialize and physically  optimize as needed to meet the response time objectives.

**How**: Organize this layer in sub-schemas (directory structure in the dbt project and database structure) to reflect the different "virtual" data marts and aces schemas. 
Create one model per object sourcing from one or more of the source image, light integrated and reuse layer. Consider that data is already pre-processed and materialized in the underlying layers and materialize only if required for user query performance or large volume of user query. 
Need to replicate logic between models in different sub-schemas should be considered as an opportunity to move the logic to the "reuse" layer.
This is the layer where data engineers have the most freedom, so keep things simple. For example: since the delta logic, history, integration being already processed earlier, consider that an incremental strategy may not yield significant performance improvements at this stage and may not be worth the operational complexity they introduce (expect some exceptions).

**Examples**
- Customer dimension sourcing from light integration and core layer: [./models/consumption/dim_customers.sql](./models/consumption/dim_customers.sql)
- Dimension directly sourced from the light integration layer: [./models/consumption/dim_payment_method.sql](./models/consumption/dim_payment_method.sql)

## What is a jaffle?
A jaffle is a toasted sandwich with crimped, sealed edges. Invented in Bondi in 1949, the humble jaffle is an Australian classic. The sealed edges allow jaffle-eaters to enjoy liquid fillings inside the sandwich, which reach temperatures close to the core of the earth during cooking. Often consumed at home after a night out, the most classic filling is tinned spaghetti, while my personal favourite is leftover beef stew with melted cheese.

---
For more information on dbt:
- Read the [introduction to dbt](https://dbt.readme.io/docs/introduction).
- Read the [dbt viewpoint](https://dbt.readme.io/docs/viewpoint).
- Join the [chat](http://slack.getdbt.com/) on Slack for live questions and support.
---
