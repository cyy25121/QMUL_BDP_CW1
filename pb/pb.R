library(ggplot2)

pb.hist = read.csv("result/time",header = FALSE,sep = "\t", col.names = c("date", "occurrence"), colClasses = c("character", "numeric"))
pb.histp <- pb.hist[order(pb.hist$date), ]
pb.histp$date <- as.Date(pb.histp$date, format = '%m%d')

ggplot(data=pb.histp, aes(x=date, y=occurrence)) + 
  geom_bar(stat="identity", fill="gray") + 
  geom_line(colour = "steelblue", size = 1.1) +
  geom_point(colour="steelblue", shape=21, fill="white") + 
  labs(x = "Date", y = "Frequence", title = "Tweets Time Analysis") + 
#  geom_text(aes(label=occurrence))+
  scale_x_date(date_labels = "%b%d", date_breaks = "5 day") + 
  annotate("text", x = as.Date('2016-08-06'), y = 1900000, label = "(Aug 06, 1888097)", size = 4)
