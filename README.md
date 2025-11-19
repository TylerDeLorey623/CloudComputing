# Cloud Computing Projects
#### Overall Description:
This repository stores the cloud-based projects completed during my time taking the masters level Cloud Computing class at Marist University under R. O. Topaloglu.
#### Projects:
- **Project 1: NewsAPI Project**: 
A Golang program calls a programming API and delivers information to consumers as Docker images. The user is interested in searching for news on a topic of their choice. 
They will enter few words as their search term, in which this data is collected and news related to that search topic will be presented using newsAPI.
The user will also enter two numbers, one defining how many days of news to be searched. For example, entering 2 will search for news today and yesterday.
SQLite was implemented to buffer the results, storing them in a database. 
If a new user uses the same search query, the existing results will be read from the database instead of reaching the API. 
However if the user increased the time period or the news count limit, new results will be added to the database and the API needs to be used. 

- **Project 2: Weather Forecast Framework**: In this project, I developed a framework for weather monitoring. I used the API from openweathermap.org. Users can enter how many days to forecast and zipcode. Weather data will be served through a Kafka channel. An evaluator will check whether certain alerts should be triggered, such as high or low temperature alerts, wind alerts, etc. Prometheus will be used for time-series data. Grafana will be used for any plots. This framework also uses Golang and Docker images/containers to run.

- **Project 3: The Debate Between LLMs (COMING SOON)**: This project targets the Interfaith Fellowship program partial requirements. The program highlights importance of dialogue and openness to world views. You will pick a topic of importance to you, including a political or social topic where you anticipate different religions could have different views. Then each of the two LLM’s will take roles of a person representing a specific and different religion of your choice. These two LLM’s will discuss the topic provided by you. Each LLM will be given roughly equal time to present their point and perhaps propose a solution to the discussion topic. Once the debate starts, we should be able to view the conversation live. You should be able to change the discussion topic, the religion assignments, debate duration (or word/token limit), turn time (or word/token limit) and start a new debate. A third LLM will not follow a religion and will just judge the discussion. Again, this project also uses Golang and Docker images/containers to run.

More specifications about these projects are found under its Documentation.

#### How to Deploy these Projects:
All of these projects use Docker. Each project has its own documentation, where they have specifications for how to run them.