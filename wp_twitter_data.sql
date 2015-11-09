CREATE TABLE `wp_twitter_data` (
  `id` bigint(255) NOT NULL AUTO_INCREMENT,
  `userid` bigint(50) NOT NULL,
  `event` varchar(25) NOT NULL,
  `JSONdata` varchar(10000) NOT NULL,
  `created_datetime` varchar(50) NOT NULL,
  PRIMARY KEY (`id`)
);
