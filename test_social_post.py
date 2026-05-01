"""

** test_social_post.py **

Dummy test script for nbim_social_post.py.
Tests the post formatting and truncation logic with dummy data
containing seven deviating votes, without connecting to the database.

"""

from nbim_social_post import format_post


# ──────────────────────────────────────────────
# Dummy data
# ──────────────────────────────────────────────

DUMMY_MEETING = {
    "id":           1962910,
    "type":         "Annual",
    "date":         "2025-06-06",
    "company_name": "Alphabet Inc.",
}

DUMMY_VOTES = [
    {
        "proposal_text":    "Approve Recapitalization Plan for all Stock to Have One-vote per Share",
        "proponent":        "Shareholder",
        "management_rec":   "Against",
        "vote_instruction": "For",
    },
    {
        "proposal_text":    "Report on Meeting 2030 Climate Goals and Emissions Reduction Strategy",
        "proponent":        "Shareholder",
        "management_rec":   "Against",
        "vote_instruction": "For",
    },
    {
        "proposal_text":    "Publish a Human Rights Impact Assessment of AI Driven Targeted Advertising",
        "proponent":        "Shareholder",
        "management_rec":   "Against",
        "vote_instruction": "For",
    },
    {
        "proposal_text":    "Adopt Metrics Evaluating YouTube Child Safety Policies and Reporting",
        "proponent":        "Shareholder",
        "management_rec":   "Against",
        "vote_instruction": "For",
    },
    {
        "proposal_text":    "Report on Risks of Discrimination in Generative AI Products and Services",
        "proponent":        "Shareholder",
        "management_rec":   "Against",
        "vote_instruction": "For",
    },
    {
        "proposal_text":    "Report on Due Diligence Process to Assess Human Rights Risks in High-Risk Countries",
        "proponent":        "Shareholder",
        "management_rec":   "Against",
        "vote_instruction": "For",
    },
    {
        "proposal_text":    "Consider Ending Participation in Human Rights Campaign Corporate Equality Index",
        "proponent":        "Shareholder",
        "management_rec":   "For",
        "vote_instruction": "Against",
    },
]


# ──────────────────────────────────────────────
# Test
# ──────────────────────────────────────────────

def run() -> None:
    post = format_post(DUMMY_MEETING, DUMMY_VOTES)

    full_text = post["intro"] + post["link"] + post["outro"] + "\n" + post["lines"]

    print("=" * 60)
    print("FORMATTED POST (%d chars):" % len(full_text))
    print("=" * 60)
    print(full_text)
    print("=" * 60)
    print("\nBreakdown:")
    print("  intro : %d chars — %r" % (len(post["intro"]), post["intro"]))
    print("  link  : %d chars — %r" % (len(post["link"]), post["link"]))
    print("  outro : %d chars — %r" % (len(post["outro"]), post["outro"]))
    print("  lines : %d chars — %r" % (len(post["lines"]), post["lines"]))
    print("\nTotal : %d chars" % len(full_text))


if __name__ == "__main__":
    run()
