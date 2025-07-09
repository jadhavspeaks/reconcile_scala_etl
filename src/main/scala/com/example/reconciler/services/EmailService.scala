package com.example.reconciler.services

import com.example.reconciler.config.EmailConfig
import com.example.reconciler.models._ // For ReconciliationJobSummary and its contents
import org.apache.commons.mail.HtmlEmail
// import org.apache.commons.mail.EmailAuthenticator // Interface - Removed to see if it resolves issue
import org.apache.commons.mail.DefaultAuthenticator // Class implementing EmailAuthenticator
import org.slf4j.LoggerFactory // Added for logging
import scala.util.{Try, Success => TrySuccess, Failure => TryFailure}

class EmailService {
  private val logger = LoggerFactory.getLogger(getClass)

  def sendReconReport(summary: ReconciliationJobSummary, emailConfig: EmailConfig): Try[Unit] = {
    if (!emailConfig.enabled) {
      logger.info("Email notifications are disabled.")
      return TrySuccess(())
    }

    Try {
      val email = new HtmlEmail()
      email.setHostName(emailConfig.smtpHost)
      email.setSmtpPort(emailConfig.smtpPort)

      emailConfig.smtpUser.zip(emailConfig.smtpPassword).headOption.foreach {
        case (user, pass) =>
          email.setAuthenticator(new DefaultAuthenticator(user, pass))
      }

      emailConfig.starttlsEnabled.foreach(email.setStartTLSEnabled) // Use setStartTLSEnabled for commons-email

      email.setFrom("recon-framework@example.com", "Data Reconciliation Framework") // Set a default FROM or make it configurable
      emailConfig.recipients.foreach(email.addTo)

      val subject = s"${emailConfig.subjectPrefix} - ${summary.jobName} - Status: ${summary.overallStatus}"
      email.setSubject(subject)

      // Generate HTML content
      val htmlContent = generateHtmlContent(summary)
      email.setHtmlMsg(htmlContent)

      // Fallback text content
      email.setTextMsg(s"Please view this email in an HTML-compatible client to see the full reconciliation report for ${summary.jobName}.")

      logger.info(s"Sending reconciliation email report for job ${summary.jobId} to ${emailConfig.recipients.mkString(", ")}...")
      email.send()
      logger.info(s"Reconciliation email report sent successfully for job ${summary.jobId}.")
    }
  }

  private def generateHtmlContent(summary: ReconciliationJobSummary): String = {
    // Basic HTML structure. This can be significantly improved with CSS and more detailed formatting.
    val sb = new StringBuilder
    sb.append("<html><body>")
    sb.append(s"<h1 style='color:${statusColor(summary.overallStatus)};'>Reconciliation Report: ${summary.jobName}</h1>")
    sb.append(s"<p><strong>Job ID:</strong> ${summary.jobId}</p>")
    sb.append(s"<p><strong>Overall Status:</strong> <span style='font-weight:bold; color:${statusColor(summary.overallStatus)}'>${summary.overallStatus}</span></p>")
    sb.append(s"<p><strong>Start Time:</strong> ${new java.util.Date(summary.startTime)}</p>")
    summary.endTime.foreach(et => sb.append(s"<p><strong>End Time:</strong> ${new java.util.Date(et)}</p>"))
    summary.errorMessages.headOption.foreach { _ =>
      sb.append("<h2>Error Messages:</h2><ul>")
      summary.errorMessages.foreach(err => sb.append(s"<li><pre>${htmlEscape(err)}</pre></li>"))
      sb.append("</ul>")
    }

    summary.rowCountResult.foreach { rc =>
      sb.append("<h2>Row Count Comparison:</h2>")
      sb.append(s"<p>Status: <span style='font-weight:bold; color:${statusColor(rc.status)}'>${rc.status}</span></p>")
      sb.append(s"<p>Source Rows: ${rc.sourceRowCount}, Target Rows: ${rc.targetRowCount}, Difference: ${rc.difference}</p>")
      sb.append(s"<p>${htmlEscape(rc.summaryMessage)}</p>")
    }

    summary.schemaReconResult.foreach { sr =>
      sb.append("<h2>Schema Reconciliation:</h2>")
      sb.append(s"<p>Status: <span style='font-weight:bold; color:${statusColor(sr.status)}'>${sr.status}</span></p>")
      sb.append(s"<p>${htmlEscape(sr.summaryMessage)}</p>")
      if(sr.fieldComparisons.exists(!_.isMatch)){
        sb.append("<table border='1'><tr><th>Field</th><th>Source Type</th><th>Target Type</th><th>Match</th><th>Remarks</th></tr>")
        sr.fieldComparisons.filter(!_.isMatch).foreach { fc =>
          sb.append(s"<tr><td>${fc.fieldName}</td><td>${fc.sourceDataType.getOrElse("N/A")}</td><td>${fc.targetDataType.getOrElse("N/A")}</td><td style='color:${if(fc.isMatch) "green" else "red"}'>${fc.isMatch}</td><td>${htmlEscape(fc.remarks.getOrElse(""))}</td></tr>")
        }
        sb.append("</table>")
      }
    }

    summary.dataMatchingResult.foreach { dmr =>
      sb.append("<h2>Data Matching (Key-based):</h2>")
      sb.append(s"<p>Status: <span style='font-weight:bold; color:${statusColor(dmr.status)}'>${dmr.status}</span></p>")
      sb.append(s"<p>Source Total: ${dmr.sourceRowCount}, Target Total: ${dmr.targetRowCount}</p>")
      sb.append(s"<p>Matched Keys: ${dmr.matchedKeyCount}</p>")
      sb.append(s"<p>Source-Only Keys: ${dmr.sourceOnlyKeyCount}</p>")
      sb.append(s"<p>Target-Only Keys: ${dmr.targetOnlyKeyCount}</p>")
      sb.append(s"<p>${htmlEscape(dmr.summaryMessage)}</p>")
    }

    summary.valueComparisonResult.foreach { vcr =>
      sb.append("<h2>Column Value Comparison (for Matched Keys):</h2>")
      sb.append(s"<p>Status: <span style='font-weight:bold; color:${statusColor(vcr.status)}'>${vcr.status}</span></p>")
      sb.append(s"<p>Total Rows Compared: ${vcr.totalComparedRows}</p>")
      sb.append(s"<p>Rows with Mismatches: ${vcr.mismatchedRowCount}</p>")
      if(vcr.mismatchedRowCount > 0) {
        sb.append("<h3>Mismatch Counts per Column:</h3><ul>")
        vcr.columnMismatchCounts.foreach { case (col, count) => sb.append(s"<li>${htmlEscape(col)}: $count</li>") }
        sb.append("</ul>")
        // Could add link to HDFS/Hive table for full mismatch details here
      }
      sb.append(s"<p>${htmlEscape(vcr.summaryMessage)}</p>")
    }

    summary.businessRuleResults.foreach { brrList =>
      if(brrList.nonEmpty) {
        sb.append("<h2>Business Rule Validation:</h2>")
        sb.append("<table border='1'><tr><th>Rule Name</th><th>Status</th><th>Query</th><th>Expected</th><th>Actual</th><th>Remarks</th></tr>")
        brrList.foreach { br =>
          sb.append(s"<tr><td>${htmlEscape(br.ruleName)}</td><td style='font-weight:bold; color:${statusColor(br.status)}'>${br.status}</td><td><pre>${htmlEscape(br.query)}</pre></td><td>${htmlEscape(br.expectedResult.getOrElse("N/A"))}</td><td>${htmlEscape(br.actualResult.getOrElse("N/A"))}</td><td>${htmlEscape(br.remarks.getOrElse(""))}</td></tr>")
        }
        sb.append("</table>")
      }
    }

    // New section for Business Rule Comparison Result (SQL result vs Target)
    summary.businessRuleComparisonResult.foreach { brcr =>
      sb.append("<h2>Business Rule Comparison (SQL Result vs. Target):</h2>")
      sb.append(s"<p>Status: <span style='font-weight:bold; color:${statusColor(brcr.status)}'>${brcr.status}</span></p>")
      sb.append(s"<p>Total Rows Compared in Target (after join with rule result): ${brcr.totalComparedRows}</p>")
      sb.append(s"<p>Rows with Mismatches: ${brcr.mismatchedRowCount}</p>")
      if(brcr.mismatchedRowCount > 0) {
        sb.append("<h3>Mismatch Counts per Column (Rule vs. Target):</h3><ul>")
        brcr.columnMismatchCounts.foreach { case (col, count) => sb.append(s"<li>${htmlEscape(col)}: $count</li>") }
        sb.append("</ul>")
        // Could add link to HDFS/Hive table for full mismatch details for this comparison
      }
      sb.append(s"<p>${htmlEscape(brcr.summaryMessage)}</p>")
    }

    sb.append("</body></html>")
    sb.toString()
  }

  private def statusColor(status: ReconStatus): String = status match {
    case Success => "green"
    case Failure => "red"
    case PartialSuccess => "orange" // Or some other color
    case _ => "black"
  }

  private def htmlEscape(text: String): String = {
    // Basic HTML escaping for text content
    text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\"", "&quot;")
        .replace("'", "&#39;")
  }

}
