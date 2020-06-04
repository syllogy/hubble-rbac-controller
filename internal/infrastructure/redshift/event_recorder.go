package redshift

import log "github.com/sirupsen/logrus"

type EventRecorder struct {
	events []ApplyEventType
}

func (e *EventRecorder) Handle(eventType ApplyEventType, name string) {
	log.Infof("Event %s:%s occurred", eventType.ToString(), name)
	e.events = append(e.events, eventType)
}

func (e *EventRecorder) Reset() {
	e.events = []ApplyEventType{}
}

func (e *EventRecorder) CountAll() int {
	return len(e.events)
}

func (e *EventRecorder) Count(eventType ApplyEventType) int {
	result := 0
	for _, event := range e.events {
		if event == eventType {
			result += 1
		}
	}
	return result
}

func (e *EventRecorder) HasHappened(eventTypes ...ApplyEventType) bool {

	for _, eventType := range eventTypes {
		if e.Count(eventType) == 0 {
			return false
		}
	}
	return true
}