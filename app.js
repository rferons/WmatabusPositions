const fetch = require('node-fetch');
const {Client} = require('@elastic/elasticsearch');
const moment = require('moment-timezone');

class BusPositions {
    client = null;
    index = 'buses';

    constructor() {
        this.client = new Client({node: 'http://localhost:9200'});
    }

    async hasIndex() {
        return await this.client.indices.exists({index: this.index})
            .then(response => {
                return response.body
            })
    }

    async createIndex() {
        return await this.client.indices.create({
            index: this.index,
            body: {
                mappings: {
                    properties: {
                        VehicleID: { type: "text" },
                        location: { type: "geo_point" },
                        Deviation: { type: "integer" },
                        DateTime: { type: "date" },
                        TripID: { type: "text" },
                        RouteID: { type: "text" },
                        DirectionNum: { type: "integer" },
                        DirectionText: { type: "text" },
                        TripHeadsign: { type: "text" },
                        TripStartTime: { type: "date" },
                        TripEndTime: { type: "date" },
                        BlockNumber: { type: "text" }
                    }
                }
            }
        })
            .then(response => {
                return response.body
            })
    }

    async updateBusPositions() {
        console.log('Getting new positions');
        let busPositions = await this.getNewBusPositions();
        this.addPositionsToIndex(busPositions)
    }

    getNewBusPositions() {
        return fetch('https://api.wmata.com/Bus.svc/json/jBusPositions', {
            headers: {
                api_key: 'ac5fcd90594f4d72bd5cc672bb4f62f3'
            }
        })
            .then(results => results.json())
            .then(body => {
                if (body && body.BusPositions.length > 0) {
                    return body.BusPositions
                } else {
                    console.log(body);
                    throw new Error('No bus positions returned')
                }
            });
    }

    addPositionsToIndex(positions) {
        positions.forEach(position => {
            // console.log(position.DateTime);
            position.location = {
                lat: position.Lat,
                lon: position.Lon
            };
            position.DateTime = this.parseDate(position.DateTime);
            position.TripStartTime = this.parseDate(position.TripStartTime);
            position.TripEndTime = this.parseDate(position.TripEndTime);
            this.client.index({
                index: this.index,
                body: position
            }).catch(error =>  {
                console.log(error.body)
            })
        })
    }

    parseDate(date) {
        return moment.tz(date, 'America/New_York').toISOString();
    }

    async main() {
        let hasIndex = await this.hasIndex();
        if (hasIndex === false) {
            await this.createIndex();
        }

        this.updateBusPositions();
    }
}

const recursiveRun = () => {
    // call the main function
    let BusPositionsApi = new BusPositions();
    BusPositionsApi.main().catch(error => console.error(error));

    setTimeout(recursiveRun, 10 * 1000);
};

recursiveRun();
