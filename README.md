This is an experimental project to build a client and analysis tool for voting data retrieved from the Norges Bank Investment Management Voting Records API. It builds on the Python client by [Ömer Faruk Demir](https://github.com/omerfarukdemir). 

The project is currently under development. It can be set up in different ways:
- Run directly as Python scripts "nbim_companies_update.py" and "nbim_meetings_update.py" which fetches company and meeting data, respectively, and adds to / updates a MySQL database running on localhost. This gives the possibility to capture data into a database for further analysis.
- Run as a bot that posts information to social emdia based on meetings in database. This is currently implemented in the [NBIM Vote Alert](https://bsky.app/profile/nbim-vote.bsky.social) bot on Bluesky (script nbim_social_post.py)
- Composed from Dockerfile and run as a container. This creates a FastAPI access point to the NBIM client, set up to listen on port 8088. At the moment, this only provides access directly to the returned JSON data and does not interact with the database.

Further down the line, I want to make the data accessible through a frontend which interacts with the database and/or the API through the client. The idea is for a HTML/JS based interface - _this is yet to be developed_.

Feel free to use any elements of this that may be useful. I have added some documentation to the scripts but it is currently a bit messy and the scripts are not really streamlined...

**Note** that to access the voting data, you will need to [apply to NBIM](https://www.nbim.no/en/the-fund/responsible-investment/our-voting-records/api-access-to-our-voting/) for an API key. The API restricts the number of requests - I don't know the exact limit, but several thousand requests in short order seems to block the API key's access for 24 hours. I have therefore implemented limits in the scripts + some functionality that allows large requests to be split into acceptable chunks (for example when retrieving the full list of companies or meetings).
