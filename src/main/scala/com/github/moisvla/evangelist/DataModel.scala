package com.github.moisvla.evangelist


// we don't need the entire json struct
case class RSVP(group: Option[Group], event: Option[Event], rsvp_id: String)

case class Event(event_name: Option[String], time: Option[Long], event_id: Option[String], event_url: Option[String])

case class Group(group_topics: Option[List[GroupTopic]], group_city: String, group_country: String, group_name: String,
                 group_id: Option[Long], group_lat: Option[Double], group_lon: Option[Double], group_urlname: Option[String])

case class GroupTopic(urlkey: String, topic_name: Option[String])

case class CSV(group_topic: String, group_name: String, event_name: String, rsvp_id: String)
