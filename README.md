# APIServer Daemon
[![Travis](http://img.shields.io/travis/FutureGateway/geAPIServer/master.png)](https://travis-ci.org/FutureGateway/geAPIServer)
[![License](https://img.shields.io/github/license/FutureGateway/geAPIServer.svg?style?flat)](http://www.apache.org/licenses/LICENSE-2.0.txt)

RESTful API Server compliant with [CSGF APIs][specs] specifications.

This service offers the same capabilities of the API Server project with the following differences:
 - It exploits the [CSGF][CSGF]' GridEngine system to target distributed ifnrastructures
 - It supports other executor services just providing the correct interface class
 - This daemon communicates with an API Server [agent][fgAPIServer]
 - The API agent manages incoming REST calls and then instructs the GridEngine accordingly

The Principal advantages of this solutions are:

 - Backward compatibility with existing systems based on the CSGF
 - Fast provisioning of ready to go solutions
 - Fast prototyping when designing new features and components (including APIServer itself)
 - Ideal solution for existing development environments using CSGF

   [specs]: <http://docs.csgfapis.apiary.io/#reference/v1.0/application/create-a-task>
   [CSGF]: <https://www.catania-science-gateways.it>
   [fgAPIServer]: <https://github.com/FutureGateway/fgAPIServer>
