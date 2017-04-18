#Tor distributed messenger

A messenger system that uses a tor-like protocol to schedule meeting among clients. Original requester sends out initial invitation to the first participant on the list. Then the inivitation is sent to the next participant on the next, and so on, until a participant rejects the meeting or all participants accept the meeting. 

Storage is only modified when all clients reach consensus, i.e. accept the invitation. No central server is needed to process meeting.
