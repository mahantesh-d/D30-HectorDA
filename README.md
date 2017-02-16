# D30-HectorDA
[![Build Status](https://travis-ci.org/dminGod/D30-HectorDA.svg?branch=master)](https://travis-ci.org/dminGod/D30-HectorDA) [![GoDoc](https://godoc.org/github.com/dminGod/D30-HectorDA?status.svg)](https://godoc.org/github.com/dminGod/D30-HectorDA)
[![Go Report Card](https://goreportcard.com/report/github.com/dminGod/D30-HectorDA)](https://goreportcard.com/report/github.com/dminGod/D30-HectorDA)
### Overview

---

This is a middleware application that allows you to share Data as a Service (DaaS) to end consuming applications. It acts as a layer of abstraction between the underlying database and the API. Allowing you to merge data across different types of database types in a single call. Do transformations on the received data and write custom logic for each API endpoint.
It can interface with multiple types of databases and supports versioning of the API such that multiple versions of the same API can be used as the same time where each version of the same API can have different request & responses.

As this type of an application is by design meant to be extended as more applications and database types integrate with it. The design of the applicaiton is modular by design where
each layer has separation of concerns. This allows for easy extensibility of each of the layers.


### Documentation and Reference

---

API Documentation : [https://godoc.org/github.com/dminGod/D30-HectorDA](https://godoc.org/github.com/dminGod/D30-HectorDA)

Docker Image : [https://hub.docker.com/r/akshays/hectorda/](https://hub.docker.com/r/akshays/hectorda/)

### Application Architecture

---

#### Overview
The broad underlying design theme of this application is a modular approach where each layer communicates with the layers below
and above it in a structured format and clearly stated separation of concenrs. This allows for easily adding modules on any of the layers and extending support for different types of technologies.

![alt text](https://raw.githubusercontent.com/dminGod/D30-HectorDA/master/references/architecture_diagram.jpg "Architecture Overview")

#### Server Layer
This layer is responsible to listening for requests and handles both requests and responses. Current server that are supported
is Protobuf over HTTP. The requests are accepted and converted to a standard request object, and sent to the layer below. As a response the layer
below it returns back a standard response object. This response object is then used by this layer to form a response back to the
caller in the server original format.

Because the request objects for the layer below and the response object returned from the layer below are standard only code for the actual
server implementation would be needed to extend this layer to accept any other type of format.

This layer can easily be extended to support REST / SOAP / MQTT in the future.

#### Request Response Abstract
This layer serves 2 purposes.

1) As a standardization layer between the Different servers and the layer below so the servers and the implementation
don't need to be tightly coupled and the code in the layers below can be reused.

2) This is a layer through which all traffic is expected to flow through. Both the request and response will flow through this layer.
This allows for(All are planned features) :
- All requests wide security validation & dropping requests that are not authorised.
- Hooks and logic to integrate logging systems, calls to monitoring and alarm systems.
- Transformation or enriching of all responses before they are returned back to the caller.

This layer accepts a standard Request object and returns a fixed Response object to the Server Layer


#### Application Custom Logic
Based on the application and version of the calling application the request is routed to the right method. Each web request typically
would correspond to a custom logic method that will receive the request data, make queries to data sources using the layers below.
These could be multiple queries if necessary, and transformations on the fetched data. Any API specific logic would be written here.

There are modules available here as helpers that give information about API call field to table mapping reference. Query generation
helpers for different types of databases.

This is the major part that would need to be extended to support future applications.


#### Database Abstract Layer
The purpose of this layer is to have a standard request and response format for all types of databases. This layer simplifies the creation of
database drivers. This layer exists to keep things modular within the application and also simplifies the creation of driver layer to support more databases.

As all queries pass through this layer, hooks can easily be written to extend this layer to support logging of all queires or sending certain data to
monitoring / alerting systems.

As both the request and response flow though this layer, logging of performance data for all queries can also be done. Because Go has great support
for concurrency and multi-threading, all these logs and requests can be asynchronous. Where the impact on request times would not reduced greatly
as the request for data and logging would happen in parallel.


#### Database Driver Layer
This is the layer where the actual calls to the database are made. This layer contains all the logic necessary to interface with the
underlying databases.

This layer accepts standard requests from the Database Abstract Layer and returns responses in a standard format and thus additional
database drivers can be added here.

Connection pooling for databases would be implemented here as well.






