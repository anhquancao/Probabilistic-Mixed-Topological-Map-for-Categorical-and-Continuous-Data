data <- read.table("src/resources/s1.txt")
data <- head(data,1000)
plot(x = data$V1, y=data$V2)

probs <- read.csv("./out/Mon Jul 23 16:39:23 ICT 2018-prob",header = FALSE)
sum(probs)
