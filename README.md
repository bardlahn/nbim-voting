This is an experimental project to build a client and analysis tool for voting data retrieved from the Norges Bank Investment Management Voting Records API. It builds on the Python client by [Ömer Faruk Demir](https://github.com/omerfarukdemir). 

The project is currently under development. It can be set up in three ways:
- Composed from Dockerfile and run as a container. This creates a FastAPI access point to the NBIM client, set up to listen on port 8088. At the moment, this only provides access directly to the returned JSON data and does not interact with the database
- Run directly as Python scripts "update_companies.py" and "update_meetings.py" which fetches all company and meeting data, respectively, and adds to / updates a MySQL database running on localhost. This gives the possibility to capture data into a database for further analysis.
- Accessible through a frontend which interacts with the database and/or the API through the client. The idea is for a HTML/JS based interface - _this is yet to be developed_.

Feel free to use any elements of this that may be useful. Note that to access the voting data, you will need to [apply to NBIM](https://www.nbim.no/en/the-fund/responsible-investment/our-voting-records/api-access-to-our-voting/) for an API key.
