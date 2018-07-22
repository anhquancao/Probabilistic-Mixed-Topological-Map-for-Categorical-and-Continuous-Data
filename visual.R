data <- read.table("src/resources/s1.txt")
plot(x = data$V1, y=data$V2)

items <- read.csv("./out/Thu Jul 19 17:10:05 ICT 2018-items",header = FALSE)
matrix = data.matrix(items)
heatmap(matrix,)
