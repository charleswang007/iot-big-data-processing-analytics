# iot-big-data-processing-analytics

## Background

Final Project for UCSC Extenstion Course *Internet of Things (IoT) Big Data Processing and Analytics*

Course Description: 

[https://www.ucsc-extension.edu/certificate-program/offering/internet-things-big-data-processing-and-analytics](https://www.ucsc-extension.edu/certificate-program/offering/internet-things-big-data-processing-and-analytics)

## Project

Physics, especially gravity, plays a huge part in how roller coasters work. Potential and kinetic energies are constantly converting to each other while the roller coaster moves up and down the track. On the other hand, inertia (Newton's first law of motion) keeps the coaster car a forward velocity even when it is moving up the track, opposite the force of gravity. In this project, IOT devices capture acceleration (G-force) and turning speed (angular velocity) during the course of a roller coaster run when gravity and inertia apply. Barometric pressure (in hPa) is also captured as the coaster's altitude changes throughout the ride. Data sampling rate is 1 measurement per second. Kafka_6.5.log shows 120 data points collected in 120 seconds (2 minutes). G-force flunctuates from 0 to 7, angular velocity from 0 to 1 (unit: rad/s), and barometric-pressure from 1000 to 1200 (unit: hPa). Streaming data is processed by pyspark through kafka. Whenver there's a new roller coaster data steam, we update the current average and max value for each of the three measurements: G-force, angular velocity, and barometric measure. This experiment can assist in understanding roller coaster physics as well as helping amusement park engineer design faster and more exciting roller coasters, under a resonably safe premises.
