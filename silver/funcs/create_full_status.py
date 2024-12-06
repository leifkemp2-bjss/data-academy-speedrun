# Databricks notebook source
def createFullStatus(status):
    statusValue = None
    examinerValue = None
    reasonValue = None

    try:
        statusValue = status.status
    except Exception:
        pass
    try:
        examinerValue = status.examiner
    except Exception:
        pass
    try:
        reasonValue = status.reason
    except Exception:
        pass

    return Row(status=statusValue, examiner=examinerValue, reason=reasonValue)
