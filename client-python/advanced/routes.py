#!/usr/bin/env python
#
# Copyright 2016 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Example Kafka Producer.
# Reads lines from stdin and sends to Kafka.
#

from flask import Flask, request
from flask_restful import Resource, Api
from producerFunctions import ProducerFunctions
import sys

app = Flask(__name__)
api = Api(app)

class Routes(Resource):
    def post(self, params):
        body = request.json

        pf = ProducerFunctions()

        if params == 'transaction':
            result = pf.transactionHandling(str(body))
        elif params == 'transactionWithFraud':
            result = pf.transactionWithFraudHandling(str(body))
        return {"status": 200}

api.add_resource(Routes, '/<string:params>')

if __name__ == '__main__':
    app.run(debug=True)
