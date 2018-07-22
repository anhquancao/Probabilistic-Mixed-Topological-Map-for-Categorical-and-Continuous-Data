data <- read.table("src/resources/s1.txt")
data <- head(data,1000)
plot(x = data$V1, y=data$V2)

items <- read.csv("./out/Sun Jul 22 20:05:15 ICT 2018-prob",header = FALSE)
sum(items)
