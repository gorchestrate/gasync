# gasync
Minimalist serverless workflow framework built on top of Google Cloud.

It allows you to:
* Run your workflows for free
  * Cloud Run: 1 million req/month are free
  * Cloud Tasks: 1 millin req/month is free
  * Firestore: 1 GB storage + 20,000 ops/day are free
* Create fully documented, validated, readable, testable and actually working workflows in minutes, rather than weeks.
* Understand the logic behind your workflows by writing them as a code.
* Run concurrent workflows in a Go style, using Goroutines and wait conditions.

Core library that this framework is based on: https://github.com/gorchestrate/async

### Features

* Really fast workflow development
* Built-in Swagger documentation
* Built-in Diagram documentation
* Fully-serverless infrastructure setup

### Usage
Just look at example app: https://github.com/gorchestrate/pizzaapp