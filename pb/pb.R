pb.hist = read.csv("result/time",header = FALSE,sep = "\t", col.names = c("date", "occurrence"), colClasses = c("character", "numeric"))
pb.histp <- pb.hist[order(pb.hist$date), ]
pb.histp$date <- as.Date(pb.histp$date, format = '%m%d')

ggplot(data=pb.histp, aes(x=date, y=occurrence)) + 
  geom_bar(stat="identity", fill="steelblue") + 
  geom_line() +
#  geom_text(aes(label=occurrence))+
  scale_x_date(date_labels = "%b%d", date_breaks = "5 day")
