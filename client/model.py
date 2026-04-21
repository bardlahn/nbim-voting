from dataclasses import dataclass
from typing import Dict, Any, List


@dataclass
class Meeting:
    id: int
    type: str
    date: str  # TODO: cast it to datetime

    @staticmethod
    def from_dict(meeting_dict: Dict[str, Any]):
        return Meeting(meeting_dict["meetingId"], meeting_dict["meetingType"], meeting_dict["meetingDate"])


@dataclass
class Company:
    id: int
    name: str
    isin: str
    ticker: str
    country: str
    meetings: List[Meeting]

    @staticmethod
    def from_dict(company_dict: Dict[str, Any]):
        return Company(
            company_dict["id"],
            company_dict["name"],
            company_dict["isin"],
            company_dict["Ticker"],
            company_dict["country"],
            [Meeting.from_dict(meeting_dict) for meeting_dict in company_dict["meetings"]]
        )


@dataclass
class Vote:
    item_on_agenda_id: int
    management_rec: str
    meeting_id: int
    proponent: str
    proposal_number: str
    proposal_sequence: str
    proposal_text: str
    vote_instruction: str
    voter_rationale: Any

    @staticmethod
    def from_dict(vote_dict: Dict[str, Any]):
        return Vote(
            vote_dict["itemOnAgendaId"],
            vote_dict["managementRec"],
            vote_dict["meetingId"],
            vote_dict["proponent"],
            vote_dict["proposalNumber"],
            vote_dict["proposalSequence"],
            vote_dict["proposalText"],
            vote_dict["voteInstruction"],
            vote_dict["voterRationale"]
        )


@dataclass
class MeetingWithVotes(Meeting):
    company_id: int
    company_name: str
    company_ticker: str
    isin: str
    votes: List[Vote]

    @staticmethod
    def from_dict(meeting_dict: Dict[str, Any]):
        return MeetingWithVotes(
            meeting_dict["meetingId"],
            meeting_dict["meetingType"],
            meeting_dict["meetingDate"],
            meeting_dict["companyId"],
            meeting_dict["companyName"],
            meeting_dict["companyTicker"],
            meeting_dict["isin"],
            [Vote.from_dict(vote_dict) for vote_dict in meeting_dict["meetingVotes"]]
        )
