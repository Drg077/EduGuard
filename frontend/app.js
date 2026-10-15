/**
 * EduGuard Advisor Dashboard Client Logic
 * Handles API communication, Chart.js visualizations, filtering,
 * student details, and prediction execution.
 */

const API_BASE = window.location.origin;



// State management

let currentPage = 1;

let currentLimit = 20;

let currentSearch = "";

let currentCategory = "ALL";

let currentOnlyFlagged = false;

let searchTimeout = null;



let riskDistChart = null;

let flagRatioChart = null;

let modalHistoryChart = null;



// Initialize on page load

document.addEventListener("DOMContentLoaded", () => {

  fetchSummary();

  fetchHealth();

  loadStudents();

  loadFlaggedStudents();

  loadPredictionRuns();

  loadModelInfo();



  // Auto-refresh summary & health every 30 seconds

  setInterval(() => {

    fetchSummary();

    fetchHealth();

  }, 30000);

});



// Toast notification handler

function showToast(message, type = "info") {

  const banner = document.getElementById("toast-banner");

  const msgEl = document.getElementById("toast-message");

  const iconEl = document.getElementById("toast-icon");



  banner.className = "mb-4 p-4 rounded-lg flex items-center justify-between text-xs shadow-sm transition ";

  if (type === "success") {

    banner.classList.add("bg-emerald-50", "text-emerald-900", "border", "border-emerald-200");

    iconEl.className = "fa-solid fa-circle-check text-emerald-600";

  } else if (type === "error") {

    banner.classList.add("bg-rose-50", "text-rose-900", "border", "border-rose-200");

    iconEl.className = "fa-solid fa-triangle-exclamation text-rose-600";

  } else {

    banner.classList.add("bg-indigo-50", "text-indigo-900", "border", "border-indigo-200");

    iconEl.className = "fa-solid fa-circle-info text-indigo-600";

  }



  msgEl.innerText = message;

  banner.classList.remove("hidden");

  setTimeout(hideToast, 5000);

}



function hideToast() {

  const banner = document.getElementById("toast-banner");

  banner.classList.add("hidden");

}



// Tab navigation switcher

function switchTab(tabId) {

  const tabs = ["overview", "flagged", "students", "history", "model", "add-student", "predict", "system"];

  tabs.forEach((t) => {

    const section = document.getElementById(`tab-${t}`);

    const btn = document.getElementById(`tab-btn-${t}`);

    if (section && btn) {

      if (t === tabId) {

        section.classList.remove("hidden");

        btn.classList.add("border-indigo-500", "text-white");

        btn.classList.remove("border-transparent", "text-slate-400");

      } else {

        section.classList.add("hidden");

        btn.classList.remove("border-indigo-500", "text-white");

        btn.classList.add("border-transparent", "text-slate-400");

      }

    }

  });



  if (tabId === "flagged") loadFlaggedStudents();

  if (tabId === "students") loadStudents();

  if (tabId === "history") loadPredictionRuns();

  if (tabId === "model") loadModelInfo();

}



// Fetch dashboard summary KPIs and charts

async function fetchSummary() {

  try {

    const res = await fetch(`${API_BASE}/api/summary`);

    if (!res.ok) throw new Error("Failed to fetch summary");

    const data = await res.json();



    document.getElementById("stat-total-students").innerText = data.total_students.toLocaleString();

    document.getElementById("stat-students-predicted").innerText = data.students_with_predictions.toLocaleString();

    document.getElementById("stat-students-flagged").innerText = data.students_flagged.toLocaleString();

    document.getElementById("stat-students-not-flagged").innerText = data.students_not_flagged.toLocaleString();

    document.getElementById("stat-avg-risk").innerText = (data.average_risk_score * 100).toFixed(1) + "%";

    document.getElementById("badge-flagged-count").innerText = data.students_flagged;



    if (data.latest_run) {

      document.getElementById("run-id-val").innerText = data.latest_run.run_id;

      const statusEl = document.getElementById("run-status-val");

      statusEl.innerText = data.latest_run.status;

      statusEl.className = data.latest_run.status === "COMPLETED" ? "font-bold text-emerald-600" : "font-bold text-amber-600";

      document.getElementById("run-total-val").innerText = data.latest_run.successful_predictions + " / " + data.latest_run.total_records;

      document.getElementById("run-time-val").innerText = new Date(data.latest_run.start_time).toLocaleString();

    }



    renderFlagRatioChart(data.students_flagged, data.students_not_flagged);

    if (data.risk_distribution) {

      updateRiskDistributionChart(null, data.risk_distribution);

    }

  } catch (err) {

    console.error("Summary error:", err);

  }

}



// Fetch and render health status

async function fetchHealth() {

  try {

    const res = await fetch(`${API_BASE}/api/health`);

    if (!res.ok) return;

    const health = await res.json();



    // Kafka pill

    const dotKafka = document.getElementById("dot-kafka");

    const textKafka = document.getElementById("text-kafka");

    const sysKafkaStatus = document.getElementById("sys-kafka-status");

    if (health.kafka && health.kafka.reachable) {

      dotKafka.className = "w-2 h-2 rounded-full bg-emerald-400";

      textKafka.innerText = "Kafka Connected";

      sysKafkaStatus.innerText = "CONNECTED";

      sysKafkaStatus.className = "bg-emerald-100 text-emerald-800 px-2 py-0.5 rounded text-[10px] font-bold";

    } else {

      dotKafka.className = "w-2 h-2 rounded-full bg-rose-400";

      textKafka.innerText = "Kafka Offline";

      sysKafkaStatus.innerText = "UNREACHABLE";

      sysKafkaStatus.className = "bg-rose-100 text-rose-800 px-2 py-0.5 rounded text-[10px] font-bold";

    }



    // HDFS pill

    const dotHdfs = document.getElementById("dot-hdfs");

    const textHdfs = document.getElementById("text-hdfs");

    const sysHdfsStatus = document.getElementById("sys-hdfs-status");

    if (health.hdfs && health.hdfs.reachable) {

      dotHdfs.className = "w-2 h-2 rounded-full bg-emerald-400";

      textHdfs.innerText = "HDFS Connected";

      sysHdfsStatus.innerText = "HEALTHY";

      sysHdfsStatus.className = "bg-emerald-100 text-emerald-800 px-2 py-0.5 rounded text-[10px] font-bold";

    } else {

      dotHdfs.className = "w-2 h-2 rounded-full bg-rose-400";

      textHdfs.innerText = "HDFS Offline";

      sysHdfsStatus.innerText = "UNREACHABLE";

      sysHdfsStatus.className = "bg-rose-100 text-rose-800 px-2 py-0.5 rounded text-[10px] font-bold";

    }



    // Scheduler info

    if (health.scheduler) {

      document.getElementById("sys-sched-info").innerText = health.scheduler.schedule + " (Next: " + (health.scheduler.next_run_time ? new Date(health.scheduler.next_run_time).toLocaleString() : "None") + ")";

    }

  } catch (err) {

    console.error("Health check error:", err);

  }

}



// Load paginated students table

async function loadStudents() {

  const tbody = document.getElementById("students-table-body");

  tbody.innerHTML = `\<tr>\<td colspan="7" class="py-8 text-center text-slate-400">Loading students...\</td>\</tr>`;



  let url = `${API_BASE}/api/students?page=${currentPage}&limit=${currentLimit}`;

  if (currentSearch) url += `&search=${encodeURIComponent(currentSearch)}`;

  if (currentCategory && currentCategory !== "ALL") url += `&risk_category=${currentCategory}`;

  if (currentOnlyFlagged) url += `&only_flagged=true`;



  try {

    const res = await fetch(url);

    const data = await res.json();



    document.getElementById("current-page-num").innerText = data.page;

    document.getElementById("total-pages-num").innerText = data.total_pages;

    document.getElementById("total-records-num").innerText = data.total;



    document.getElementById("btn-prev-page").disabled = data.page <= 1;

    document.getElementById("btn-next-page").disabled = data.page >= data.total_pages;



    if (!data.students || data.students.length === 0) {

      tbody.innerHTML = `\<tr>\<td colspan="7" class="py-8 text-center text-slate-400">No matching student records found.\</td>\</tr>`;

      return;

    }



    tbody.innerHTML = data.students

      .map((s) => {

        const score = s.risk_score !== null ? (s.risk_score * 100).toFixed(1) + "%" : "N/A";

        const badgeClass = s.risk_category === "High" ? "risk-high" : s.risk_category === "Moderate" ? "risk-moderate" : "risk-low";

        const flagBadge = s.is_flagged

          ? `\<span class="bg-rose-100 text-rose-800 font-bold px-2 py-0.5 rounded-full text-[10px]">\<i class="fa-solid fa-triangle-exclamation mr-1">\</i> Flagged (>0.6)\</span>`

          : `\<span class="bg-slate-100 text-slate-600 px-2 py-0.5 rounded-full text-[10px]">Normal\</span>`;



        return `

          \<tr class="hover:bg-slate-50 transition border-b border-slate-100">

            \<td class="py-3 px-4 font-mono font-medium text-slate-900">${s.student_id}\</td>

            \<td class="py-3 px-4">

              \<div class="flex items-center space-x-2">

                \<div class="w-16 bg-slate-200 rounded-full h-2">

                  \<div class="h-2 rounded-full ${s.is_flagged ? "bg-rose-500" : s.risk_score > 0.35 ? "bg-amber-500" : "bg-emerald-500"}" style="width: ${Math.min(100, (s.risk_score || 0) * 100)}%">\</div>

                \</div>

                \<span class="font-bold text-slate-700">${score}\</span>

              \</div>

            \</td>

            \<td class="py-3 px-4">

              \<span class="px-2 py-0.5 rounded text-[10px] font-bold border ${badgeClass}">${s.risk_category}\</span>

            \</td>

            \<td class="py-3 px-4">${flagBadge}\</td>

            \<td class="py-3 px-4 text-slate-600">${s.predicted_outcome || "Pending"}\</td>

            \<td class="py-3 px-4 text-slate-400">${s.created_at ? new Date(s.created_at).toLocaleDateString() : "--"}\</td>

            \<td class="py-3 px-4 text-right">

              \<button onclick="openStudentModal('${s.student_id}')" class="bg-slate-100 hover:bg-indigo-50 hover:text-indigo-600 text-slate-600 px-2.5 py-1 rounded text-xs transition">

                \<i class="fa-solid fa-eye mr-1">\</i> View

              \</button>

            \</td>

          \</tr>

        `;

      })

      .join("");

  } catch (err) {

    tbody.innerHTML = `\<tr>\<td colspan="7" class="py-8 text-center text-rose-500">Error loading student data: ${err.message}\</td>\</tr>`;

  }

}



// Load flagged students list

async function loadFlaggedStudents() {

  const tbody = document.getElementById("flagged-table-body");

  tbody.innerHTML = `\<tr>\<td colspan="6" class="py-8 text-center text-slate-400">Loading priority list...\</td>\</tr>`;



  try {

    const res = await fetch(`${API_BASE}/api/students?only_flagged=true&limit=100`);

    const data = await res.json();



    if (!data.students || data.students.length === 0) {

      tbody.innerHTML = `\<tr>\<td colspan="6" class="py-8 text-center text-slate-400">No students currently flagged for follow-up.\</td>\</tr>`;

      return;

    }



    tbody.innerHTML = data.students

      .map((s) => {

        const score = (s.risk_score * 100).toFixed(1) + "%";

        return `

          \<tr class="hover:bg-rose-50/40 transition border-b border-rose-100/60">

            \<td class="py-3 px-4 font-mono font-bold text-rose-950">${s.student_id}\</td>

            \<td class="py-3 px-4">

              \<span class="text-rose-700 font-bold text-sm">${score}\</span>

              \<span class="text-[10px] text-rose-500 ml-1">(&gt;0.60 threshold)\</span>

            \</td>

            \<td class="py-3 px-4">

              \<span class="bg-rose-100 text-rose-800 border border-rose-300 font-bold px-2 py-0.5 rounded text-[10px]">High Risk\</span>

            \</td>

            \<td class="py-3 px-4 font-medium text-slate-700">${s.predicted_outcome}\</td>

            \<td class="py-3 px-4 text-slate-400">${new Date(s.created_at).toLocaleString()}\</td>

            \<td class="py-3 px-4 text-right">

              \<button onclick="openStudentModal('${s.student_id}')" class="bg-rose-600 hover:bg-rose-700 text-white px-3 py-1 rounded text-xs shadow-sm transition">

                \<i class="fa-solid fa-user-pen mr-1">\</i> Review Case

              \</button>

            \</td>

          \</tr>

        `;

      })

      .join("");

  } catch (err) {

    tbody.innerHTML = `\<tr>\<td colspan="6" class="py-8 text-center text-rose-500">Error loading flagged list: ${err.message}\</td>\</tr>`;

  }

}



// Open student details modal

async function openStudentModal(studentId) {

  const modal = document.getElementById("student-modal");

  document.getElementById("modal-student-id").innerText = studentId;



  try {

    const res = await fetch(`${API_BASE}/api/students/${studentId}`);

    if (!res.ok) throw new Error("Student not found");

    const data = await res.json();



    const latest = data.latest_prediction;

    if (latest) {

      document.getElementById("modal-risk-score").innerText = (latest.risk_score * 100).toFixed(1) + "%";

      const riskBadge = document.getElementById("modal-risk-badge");

      riskBadge.innerText = latest.risk_category + " Risk";

      riskBadge.className = `px-3 py-1 rounded-full font-bold text-xs ${

        latest.risk_category === "High" ? "risk-high" : latest.risk_category === "Moderate" ? "risk-moderate" : "risk-low"

      }`;



      const flagBadge = document.getElementById("modal-flag-badge");

      if (latest.is_flagged) {

        flagBadge.innerText = "FLAGGED FOR ADVISOR FOLLOW-UP (>0.60)";

        flagBadge.className = "block text-[11px] font-bold text-rose-600 mt-1";

      } else {

        flagBadge.innerText = "NOT FLAGGED (Under 0.60 threshold)";

        flagBadge.className = "block text-[11px] font-medium text-slate-500 mt-1";

      }

    } else {

      document.getElementById("modal-risk-score").innerText = "No Prediction";

      document.getElementById("modal-risk-badge").innerText = "Pending";

      document.getElementById("modal-flag-badge").innerText = "";

    }



    // Render features grid

    const featuresGrid = document.getElementById("modal-features-grid");

    featuresGrid.innerHTML = Object.entries(data.features || {})

      .map(([k, v]) => `

        \<div class="bg-slate-50 p-2 rounded border border-slate-100">

          \<span class="text-slate-400 block text-[10px] truncate" title="${k}">${k}\</span>

          \<span class="font-semibold text-slate-700">${v !== null ? v : "N/A"}\</span>

        \</div>

      `)

      .join("");



    // Render historical risk chart

    renderStudentHistoryChart(data.prediction_history || []);



    modal.classList.remove("hidden");

  } catch (err) {

    showToast("Error opening student detail: " + err.message, "error");

  }

}



function closeStudentModal() {

  document.getElementById("student-modal").classList.add("hidden");

}



// Render student history chart

function renderStudentHistoryChart(history) {

  const ctx = document.getElementById("modal-history-chart").getContext("2d");

  if (modalHistoryChart) modalHistoryChart.destroy();



  if (!history || history.length === 0) {

    ctx.clearRect(0, 0, 300, 150);

    return;

  }



  const labels = history.map((h, i) => `Run #${i + 1} (${new Date(h.created_at).toLocaleDateString()})`);

  const scores = history.map((h) => h.risk_score);



  modalHistoryChart = new Chart(ctx, {

    type: "line",

    data: {

      labels: labels,

      datasets: [

        {

          label: "Dropout Risk Score",

          data: scores,

          borderColor: "#4f46e5",

          backgroundColor: "rgba(79, 70, 229, 0.1)",

          borderWidth: 2,

          fill: true,

          tension: 0.3,

          pointRadius: 4,

          pointBackgroundColor: scores.map((s) => (s > 0.6 ? "#e11d48" : "#4f46e5")),

        },

        {

          label: "Threshold Boundary (0.60)",

          data: new Array(history.length).fill(0.6),

          borderColor: "#e11d48",

          borderDash: [4, 4],

          borderWidth: 1.5,

          fill: false,

          pointRadius: 0,

        },

      ],

    },

    options: {

      responsive: true,

      maintainAspectRatio: false,

      scales: {

        y: {

          min: 0,

          max: 1.0,

          ticks: { callback: (v) => (v * 100).toFixed(0) + "%" },

        },

      },

      plugins: { legend: { position: "top", labels: { boxWidth: 12, font: { size: 10 } } } },

    },

  });

}



// Load Prediction Runs

async function loadPredictionRuns() {

  const tbody = document.getElementById("runs-table-body");

  try {

    const res = await fetch(`${API_BASE}/api/predictions/runs`);

    const data = await res.json();

    if (!data.runs || data.runs.length === 0) {

      tbody.innerHTML = `\<tr>\<td colspan="7" class="py-8 text-center text-slate-400">No prediction runs logged yet.\</td>\</tr>`;

      return;

    }



    tbody.innerHTML = data.runs

      .map((r) => {

        const statusBadge =

          r.status === "COMPLETED"

            ? `\<span class="bg-emerald-100 text-emerald-800 px-2 py-0.5 rounded font-bold text-[10px]">COMPLETED\</span>`

            : r.status === "FAILED"

            ? `\<span class="bg-rose-100 text-rose-800 px-2 py-0.5 rounded font-bold text-[10px]">FAILED\</span>`

            : `\<span class="bg-amber-100 text-amber-800 px-2 py-0.5 rounded font-bold text-[10px]">${r.status}\</span>`;



        return `

          \<tr class="hover:bg-slate-50 transition border-b border-slate-100">

            \<td class="py-3 px-4 font-mono font-medium text-slate-800">${r.run_id}\</td>

            \<td class="py-3 px-4">\<span class="bg-slate-100 text-slate-600 px-2 py-0.5 rounded text-[10px] font-medium">${r.triggered_by}\</span>\</td>

            \<td class="py-3 px-4 text-slate-500">${new Date(r.start_time).toLocaleString()}\</td>

            \<td class="py-3 px-4">${statusBadge}\</td>

            \<td class="py-3 px-4 font-semibold text-slate-700">${r.total_records}\</td>

            \<td class="py-3 px-4 font-semibold text-emerald-600">${r.successful_predictions}\</td>

            \<td class="py-3 px-4 font-semibold text-rose-600">${r.failed_predictions}\</td>

          \</tr>

        `;

      })

      .join("");

  } catch (err) {

    tbody.innerHTML = `\<tr>\<td colspan="7" class="py-8 text-center text-rose-500">Error loading run history: ${err.message}\</td>\</tr>`;

  }

}



// Load Model Info

async function loadModelInfo() {

  try {

    const res = await fetch(`${API_BASE}/api/model/info`);

    const data = await res.json();

    const meta = data.metadata || {};



    document.getElementById("model-algorithm").innerText = meta.algorithm || "GradientBoostingClassifier";

    if (meta.overall) {

      document.getElementById("model-accuracy").innerText = (meta.overall.accuracy * 100).toFixed(1) + "%";

    }

    if (meta.dropout_class) {

      document.getElementById("model-recall").innerText = (meta.dropout_class.recall * 100).toFixed(1) + "%";

      document.getElementById("model-precision").innerText = (meta.dropout_class.precision * 100).toFixed(1) + "%";

    }



    if (meta.top_features) {

      const topList = document.getElementById("top-features-list");

      topList.innerHTML = meta.top_features

        .slice(0, 7)

        .map((f, i) => `

          \<div class="flex justify-between items-center bg-slate-50 p-2 rounded border border-slate-100">

            \<span class="truncate font-medium text-slate-700 mr-2">#${i + 1} ${f.feature}\</span>

            \<span class="font-mono text-indigo-600 font-bold">${f.importance.toFixed(3)}\</span>

          \</div>

        `)

        .join("");

    }

  } catch (err) {

    console.error("Model info error:", err);

  }

}



// Trigger manual prediction run

async function triggerManualPrediction() {

  showToast("Triggering batch prediction run across student records...", "info");

  try {

    const res = await fetch(`${API_BASE}/api/predictions/run`, { method: "POST" });

    const data = await res.json();

    if (!res.ok) throw new Error(data.detail || "Run failed");



    showToast(`Prediction run finished: ${data.run_details.successful_predictions} predictions recorded.`, "success");

    fetchSummary();

    loadStudents();

    loadFlaggedStudents();

    loadPredictionRuns();

  } catch (err) {

    showToast("Error triggering predictions: " + err.message, "error");

  }

}



// Interactive Ad-hoc prediction

async function addStudent(e) {
  e.preventDefault();
  const form = document.getElementById("add-student-form");
  const submit = document.getElementById("add-student-submit");
  const resultBox = document.getElementById("add-student-result");
  const studentId = document.getElementById("add-student-id").value.trim();
  if (!studentId) { showToast("Please enter a student identifier.", "error"); return; }
  const val = (id) => Number(document.getElementById(id).value);
  const payload = {
    student_id: studentId,
    source: "FRONTEND",
    features: {
      "Age at enrollment": val("add-student-age"),
      "Admission grade": val("add-student-adm-grade"),
      "Curricular units 1st sem (approved)": val("add-student-sem1-app"),
      "Curricular units 1st sem (grade)": val("add-student-sem1-grade"),
      "Curricular units 2nd sem (approved)": val("add-student-sem2-app"),
      "Curricular units 2nd sem (grade)": val("add-student-sem2-grade"),
      "Tuition fees up to date": val("add-student-tuition"),
      "Debtor": val("add-student-debtor")
    }
  };
  submit.disabled = true;
  submit.innerHTML = '<i class="fa-solid fa-spinner fa-spin mr-1"></i>Adding...';
  resultBox.className = "mt-5 p-4 rounded-lg border text-xs bg-indigo-50 border-indigo-200 text-indigo-900";
  resultBox.textContent = "Submitting student record...";
  try {
    const res = await fetch(`${API_BASE}/api/students/ingest`, {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload)
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      const detail = typeof data.detail === "string" ? data.detail : JSON.stringify(data.detail || data);
      throw new Error(detail || `Student ingestion failed (HTTP ${res.status}).`);
    }
    resultBox.className = "mt-5 p-4 rounded-lg border text-xs bg-emerald-50 border-emerald-200 text-emerald-900";
    resultBox.textContent = `${data.message || `Student ${studentId} ingested successfully.`} A prediction has not necessarily been generated yet.`;
    showToast(`Student ${studentId} ingested successfully.`, "success");
    form.reset();
    document.getElementById("add-student-age").value = "20";
    document.getElementById("add-student-adm-grade").value = "120";
    document.getElementById("add-student-sem1-app").value = "1";
    document.getElementById("add-student-sem1-grade").value = "10.5";
    document.getElementById("add-student-sem2-app").value = "0";
    document.getElementById("add-student-sem2-grade").value = "0";
    document.getElementById("add-student-tuition").value = "1";
    document.getElementById("add-student-debtor").value = "0";
    fetchSummary(); loadStudents(); loadFlaggedStudents();
  } catch (err) {
    resultBox.className = "mt-5 p-4 rounded-lg border text-xs bg-rose-50 border-rose-200 text-rose-900";
    resultBox.textContent = `Could not add student: ${err.message}`;
    showToast(`Student ingestion failed: ${err.message}`, "error");
  } finally {
    submit.disabled = false;
    submit.innerHTML = '<i class="fa-solid fa-user-plus mr-1"></i>Add Student';
  }
}

async function runSinglePrediction(e) {

  e.preventDefault();

  const payload = {

    student_id: document.getElementById("est-id").value,

    features: {

      "Age at enrollment": parseFloat(document.getElementById("est-age").value),

      "Admission grade": parseFloat(document.getElementById("est-adm-grade").value),

      "Curricular units 1st sem (approved)": parseInt(document.getElementById("est-sem1-app").value),

      "Curricular units 1st sem (grade)": parseFloat(document.getElementById("est-sem1-grade").value),

      "Curricular units 2nd sem (approved)": parseInt(document.getElementById("est-sem2-app").value),

      "Curricular units 2nd sem (grade)": parseFloat(document.getElementById("est-sem2-grade").value),

      "Tuition fees up to date": parseInt(document.getElementById("est-tuition").value),

      "Debtor": parseInt(document.getElementById("est-debtor").value),

    },

  };



  try {

    const res = await fetch(`${API_BASE}/api/predict/single`, {

      method: "POST",

      headers: { "Content-Type": "application/json" },

      body: JSON.stringify(payload),

    });

    const pred = await res.json();

    if (!res.ok) throw new Error(pred.detail || "Prediction error");



    const box = document.getElementById("est-result-box");

    box.classList.remove("hidden");



    const scoreEl = document.getElementById("est-score-val");

    scoreEl.innerText = (pred.dropout_risk_score * 100).toFixed(1) + "%";

    scoreEl.className = `text-2xl font-bold ${pred.is_flagged ? "text-rose-600" : pred.dropout_risk_score > 0.35 ? "text-amber-600" : "text-emerald-600"}`;



    document.getElementById("est-cat-val").innerText = pred.risk_category;

    document.getElementById("est-flag-val").innerText = pred.is_flagged ? "FLAGGED (>0.60)" : "NOT FLAGGED";

    document.getElementById("est-class-val").innerText = pred.predicted_outcome;

  } catch (err) {

    showToast(err.message, "error");

  }

}



// Chart rendering functions

function renderFlagRatioChart(flagged, notFlagged) {

  const ctx = document.getElementById("chart-flag-ratio").getContext("2d");

  if (flagRatioChart) flagRatioChart.destroy();



  flagRatioChart = new Chart(ctx, {

    type: "doughnut",

    data: {

      labels: ["Flagged (>0.6)", "Not Flagged (<=0.6)"],

      datasets: [

        {

          data: [flagged, notFlagged],

          backgroundColor: ["#e11d48", "#10b981"],

          borderWidth: 2,

          borderColor: "#ffffff",

        },

      ],

    },

    options: {

      responsive: true,

      maintainAspectRatio: false,

      plugins: {

        legend: { position: "bottom", labels: { boxWidth: 12, font: { size: 10 } } },

      },

    },

  });

}



function updateRiskDistributionChart(students, precomputedBuckets) {

  const ctx = document.getElementById("chart-risk-dist").getContext("2d");

  if (riskDistChart) riskDistChart.destroy();



  let buckets = precomputedBuckets;

  if (!buckets) {

    if (!students || students.length === 0) return;

    buckets = new Array(10).fill(0);

    students.forEach((s) => {

      if (s.risk_score !== null && s.risk_score !== undefined) {

        const idx = Math.min(9, Math.floor(s.risk_score * 10));

        buckets[idx]++;

      }

    });

  }



  const labels = ["0-10%", "10-20%", "20-30%", "30-40%", "40-50%", "50-60%", "60-70%*", "70-80%*", "80-90%*", "90-100%*"];

  const bgColors = buckets.map((_, i) => (i >= 6 ? "#e11d48" : i >= 3 ? "#f59e0b" : "#10b981"));



  riskDistChart = new Chart(ctx, {

    type: "bar",

    data: {

      labels: labels,

      datasets: [

        {

          label: "Number of Students",

          data: buckets,

          backgroundColor: bgColors,

          borderRadius: 4,

        },

      ],

    },

    options: {

      responsive: true,

      maintainAspectRatio: false,

      scales: {

        y: { beginAtZero: true, ticks: { precision: 0 } },

      },

      plugins: {

        legend: { display: false },

        tooltip: {

          callbacks: {

            afterLabel: (ctx) => (ctx.dataIndex >= 6 ? "Flagged for follow-up (>0.6)" : "Standard monitoring"),

          },

        },

      },

    },

  });

}



// Filtering and pagination controls

function applyFilters() {

  currentCategory = document.getElementById("filter-risk-category").value;

  currentOnlyFlagged = document.getElementById("filter-only-flagged").checked;

  currentPage = 1;

  loadStudents();

}



function debounceSearch() {

  clearTimeout(searchTimeout);

  searchTimeout = setTimeout(() => {

    currentSearch = document.getElementById("student-search-input").value.trim();

    currentPage = 1;

    loadStudents();

  }, 300);

}



function changePage(delta) {

  currentPage += delta;

  loadStudents();

}
