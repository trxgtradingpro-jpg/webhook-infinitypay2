(function(){
        const configEl = document.getElementById("clientAreaConfigJson");
        let config = {};
        if (configEl){
          try {
            config = JSON.parse(configEl.textContent || "{}");
          } catch (_) {
            config = {};
          }
        }

        const payload = (config && typeof config === "object") ? config.capitalChart : null;
        const canvas = document.getElementById("capitalChart");
        const summaryCanvas = document.getElementById("capitalChartResumo");
        if (!payload || !payload.available || !canvas || typeof Chart === "undefined") return;

        const fmtBRL = new Intl.NumberFormat("pt-BR", {
          style: "currency",
          currency: "BRL"
        });
        const curveLineColor = "rgb(32, 224, 120)";
        const curveFillColor = "rgba(32, 224, 120, 0.26)";
        const crosshairColor = "rgba(32, 224, 120, 0.82)";
        const windowModes = payload.window_modes || {};
        const fallbackModeData = {
          id: "fallback",
          title: "Janela atual",
          description: "",
          window_start_date: payload.window_start_date,
          window_end_date: payload.window_end_date,
          labels: payload.labels || [],
          date_labels: payload.date_labels || [],
          values: payload.values || [],
          daily_values: payload.daily_values || [],
          daily_points: payload.daily_points || [],
          y_min: payload.y_min,
          y_max: payload.y_max
        };
        const modeIds = Object.keys(windowModes);
        let activeMode = payload.default_window_mode;
        if (!activeMode || !windowModes[activeMode]) {
          activeMode = modeIds.length ? modeIds[0] : "fallback";
        }

        const modeButtons = Array.from(document.querySelectorAll(".chart-mode-btn[data-chart-mode]"));
        const windowRangeText = document.getElementById("windowRangeText");
        const chartHintText = document.getElementById("chartHintText");
        const contractsSelectedEl = document.getElementById("contractsSelected");
        const currentValueTextEl = document.getElementById("currentValueText");
        const csvTotalValueTextEl = document.getElementById("csvTotalValueText");
        const contractInput = document.getElementById("contractCountInput");
        const contractLimit = Math.max(1, Number.parseInt(payload.contract_limit || 1, 10) || 1);
        const axisPaddingBase = Math.max(0, Number(payload.axis_padding_base || 100));
        const prefersReducedMotion = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
        let activeContracts = Math.max(1, Math.min(
          contractLimit,
          Number.parseInt(payload.contract_default || 1, 10) || 1
        ));

        function getModeData(modeId) {
          return windowModes[modeId] || fallbackModeData;
        }

        function roundTo2(value) {
          return Math.round((Number(value) + Number.EPSILON) * 100) / 100;
        }

        function normalizeContract(value) {
          const parsed = Number.parseInt(value, 10);
          if (!Number.isFinite(parsed)) return 1;
          return Math.max(1, Math.min(contractLimit, parsed));
        }

        function scaleValues(values) {
          return (values || []).map((value) => {
            if (value === null || value === undefined) {
              return null;
            }
            return roundTo2(Number(value) * activeContracts);
          });
        }

        function calculateYBounds(values) {
          const visibles = (values || []).filter((value) => Number.isFinite(value));
          const fallbackCurrent = roundTo2((Number(payload.current_value) || 0) * activeContracts);
          const baseValues = visibles.length ? visibles : [fallbackCurrent];
          const minimo = Math.min(...baseValues);
          const maximo = Math.max(...baseValues);
          const padding = axisPaddingBase * Math.max(1, activeContracts);

          let yMin = minimo - padding;
          let yMax = maximo + padding;

          if (Math.abs(yMax - yMin) < 0.01) {
            const fallbackPadding = padding || 100;
            yMin -= fallbackPadding;
            yMax += fallbackPadding;
          }

          return {
            y_min: roundTo2(yMin),
            y_max: roundTo2(yMax),
          };
        }

        function getScaledModeData(modeData) {
          const scaledValues = scaleValues(modeData.values || []);
          const scaledDailyValues = scaleValues(modeData.daily_values || []);
          const yBounds = calculateYBounds(scaledValues);
          return {
            ...modeData,
            values: scaledValues,
            daily_values: scaledDailyValues,
            daily_points: modeData.daily_points || [],
            y_min: yBounds.y_min,
            y_max: yBounds.y_max,
          };
        }

        function countVisibleValues(values) {
          return (values || []).filter((value) => Number.isFinite(value)).length;
        }

        function getPointRadius(values) {
          return countVisibleValues(values) <= 1 ? 4 : 0;
        }

        function animateCurrencyValue(targetEl, fromValue, toValue) {
          if (!targetEl) return;
          const nextValue = Number.isFinite(toValue) ? toValue : 0;
          const startValue = Number.isFinite(fromValue) ? fromValue : nextValue;

          if (prefersReducedMotion || Math.abs(nextValue - startValue) < 0.01) {
            targetEl.textContent = fmtBRL.format(nextValue);
            targetEl.dataset.currentValue = String(nextValue);
            targetEl.classList.remove("is-pulsing");
            return;
          }

          const duration = 560;
          const startedAt = performance.now();
          targetEl.classList.add("is-pulsing");

          function step(now) {
            const progress = Math.min(1, (now - startedAt) / duration);
            const eased = 1 - Math.pow(1 - progress, 3);
            const value = startValue + ((nextValue - startValue) * eased);
            targetEl.textContent = fmtBRL.format(value);

            if (progress < 1) {
              requestAnimationFrame(step);
              return;
            }

            targetEl.textContent = fmtBRL.format(nextValue);
            targetEl.dataset.currentValue = String(nextValue);
            setTimeout(() => targetEl.classList.remove("is-pulsing"), 180);
          }

          requestAnimationFrame(step);
        }

        function refreshContractIndicators() {
          if (contractsSelectedEl) {
            contractsSelectedEl.textContent = String(activeContracts);
          }
          if (currentValueTextEl) {
            const currentValue = (Number(payload.current_value) || 0) * activeContracts;
            currentValueTextEl.textContent = fmtBRL.format(currentValue);
          }
          if (csvTotalValueTextEl) {
            const csvTotalValue = (Number(payload.csv_total_value) || 0) * activeContracts;
            const previousValue = Number(csvTotalValueTextEl.dataset.currentValue);
            animateCurrencyValue(csvTotalValueTextEl, previousValue, csvTotalValue);
          }
          if (contractInput) {
            contractInput.value = String(activeContracts);
          }
        }

        function getHintText(modeData) {
          if (!modeData) {
            return "Passe o mouse sobre a linha para ver a data e o valor do dia.";
          }
          if (modeData.id === "forward30") {
            if (payload.marker_detected) {
              return "Janela de 30 dias posteriores. Mostra do início do plano até o dia marcado com g no CSV (dia " + payload.marker_day + "). Contratos aplicados: " + activeContracts + ".";
            }
            return "Janela de 30 dias posteriores. Mostra do início do plano até o último dia atualizado. Contratos aplicados: " + activeContracts + ".";
          }
          if (modeData.id === "back30") {
            return "Janela de 30 dias anteriores. O dia atual fica no fim do grafico. Contratos aplicados: " + activeContracts + ".";
          }
          return "Janela da curva no periodo selecionado. Contratos aplicados: " + activeContracts + ".";
        }

        const initialModeData = getScaledModeData(getModeData(activeMode));
        const initialPointRadius = getPointRadius(initialModeData.values);
        let renderedModeData = initialModeData;

        const crosshairPlugin = {
          id: "crosshairPlugin",
          afterDatasetsDraw(chart) {
            const active = chart.tooltip && chart.tooltip.getActiveElements
              ? chart.tooltip.getActiveElements()
              : [];
            if (!active || !active.length) return;

            const ctx = chart.ctx;
            const area = chart.chartArea;
            const point = active[0].element;
            const x = point.x;
            const y = point.y;

            ctx.save();
            ctx.strokeStyle = crosshairColor;
            ctx.lineWidth = 1;
            ctx.setLineDash([4, 4]);

            ctx.beginPath();
            ctx.moveTo(x, area.top);
            ctx.lineTo(x, area.bottom);
            ctx.stroke();

            ctx.beginPath();
            ctx.moveTo(area.left, y);
            ctx.lineTo(area.right, y);
            ctx.stroke();
            ctx.restore();
          }
        };

        const chart = new Chart(canvas, {
          type: "line",
          data: {
            labels: initialModeData.labels || [],
            datasets: [{
              label: "Capital total (1 contrato)",
              data: initialModeData.values || [],
              borderColor: curveLineColor,
              backgroundColor: curveFillColor,
              fill: true,
              tension: 0,
              borderWidth: 2,
              pointRadius: initialPointRadius,
              pointHoverRadius: initialPointRadius > 0 ? 7 : 4,
              pointHitRadius: initialPointRadius > 0 ? 12 : 6,
              pointHoverBackgroundColor: curveLineColor,
              pointHoverBorderColor: "#0f2d24"
            }]
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
              mode: "index",
              intersect: false
            },
            plugins: {
              legend: {
                display: false
              },
              tooltip: {
                displayColors: false,
                callbacks: {
                  title(items) {
                    if (!items || !items.length) return "";
                    const idx = items[0].dataIndex;
                    const modeData = renderedModeData || getModeData(activeMode);
                    const dateLabel = (modeData.date_labels || [])[idx] || "";
                    const dayLabel = (modeData.labels || [])[idx] || "";
                    return dateLabel + " (Dia " + dayLabel + ")";
                  },
                  label(context) {
                    const idx = context.dataIndex;
                    const valor = Number(context.parsed.y);
                    if (!Number.isFinite(valor)) {
                      return "Sem curva para esse dia.";
                    }
                    const sufixo = activeContracts > 1 ? "s" : "";
                    const lines = [];
                    const dailyValue = Number((renderedModeData.daily_values || [])[idx]);
                    const dailyPointsRaw = (renderedModeData.daily_points || [])[idx];

                    if (Number.isFinite(dailyValue)) {
                      if (payload.csv_value_mode === "points" && dailyPointsRaw !== null && dailyPointsRaw !== undefined && Number.isFinite(Number(dailyPointsRaw))) {
                        const dailyPointsNum = Number(dailyPointsRaw);
                        const dailyPointsText = Number.isInteger(dailyPointsNum)
                          ? String(dailyPointsNum)
                          : dailyPointsNum.toFixed(2).replace(".", ",");
                        const signedPoints = (dailyPointsNum > 0 ? "+" : "") + dailyPointsText;
                        lines.push("Resultado do dia: " + signedPoints + " pts (" + fmtBRL.format(dailyValue) + ")");
                      } else {
                        lines.push("Resultado do dia: " + fmtBRL.format(dailyValue));
                      }
                    }

                    lines.push("Capital total (" + activeContracts + " contrato" + sufixo + "): " + fmtBRL.format(valor));
                    return lines;
                  }
                }
              }
            },
            scales: {
              x: {
                grid: {
                  color: "rgba(120, 142, 184, 0.18)"
                },
                ticks: {
                  color: "#9db0d4",
                  maxTicksLimit: 10
                },
                title: {
                  display: true,
                  text: "Dias",
                  color: "#9db0d4"
                }
              },
              y: {
                min: initialModeData.y_min ?? payload.y_min,
                max: initialModeData.y_max ?? payload.y_max,
                grid: {
                  color: "rgba(120, 142, 184, 0.18)"
                },
                ticks: {
                  color: "#9db0d4",
                  callback(value) {
                    return fmtBRL.format(value);
                  }
                },
                title: {
                  display: true,
                  text: "Capital acumulado (R$)",
                  color: "#9db0d4"
                }
              }
            }
          },
          plugins: [crosshairPlugin]
        });
        let summaryChart = null;
        if (summaryCanvas) {
          summaryChart = new Chart(summaryCanvas, {
            type: "line",
            data: {
              labels: initialModeData.labels || [],
              datasets: [{
                label: "Capital total (1 contrato)",
                data: initialModeData.values || [],
                borderColor: curveLineColor,
                backgroundColor: curveFillColor,
                fill: true,
                tension: 0,
                borderWidth: 2,
                pointRadius: initialPointRadius,
                pointHoverRadius: initialPointRadius > 0 ? 6 : 4,
                pointHitRadius: initialPointRadius > 0 ? 10 : 6,
                pointHoverBackgroundColor: curveLineColor,
                pointHoverBorderColor: "#0f2d24"
              }]
            },
            options: {
              responsive: true,
              maintainAspectRatio: false,
              interaction: {
                mode: "index",
                intersect: false
              },
              plugins: {
                legend: {
                  display: false
                },
                tooltip: {
                  displayColors: false,
                  callbacks: {
                    title(items) {
                      if (!items || !items.length) return "";
                      const idx = items[0].dataIndex;
                      const modeData = renderedModeData || getModeData(activeMode);
                      const dateLabel = (modeData.date_labels || [])[idx] || "";
                      const dayLabel = (modeData.labels || [])[idx] || "";
                      return dateLabel + " (Dia " + dayLabel + ")";
                    },
                    label(context) {
                      const valor = Number(context.parsed.y);
                      if (!Number.isFinite(valor)) {
                        return "Sem curva para esse dia.";
                      }
                      return "Capital total: " + fmtBRL.format(valor);
                    }
                  }
                }
              },
              scales: {
                x: {
                  grid: {
                    color: "rgba(120, 142, 184, 0.18)"
                  },
                  ticks: {
                    color: "#9db0d4",
                    maxTicksLimit: 10
                  },
                  title: {
                    display: true,
                    text: "Dias",
                    color: "#9db0d4"
                  }
                },
                y: {
                  min: initialModeData.y_min ?? payload.y_min,
                  max: initialModeData.y_max ?? payload.y_max,
                  grid: {
                    color: "rgba(120, 142, 184, 0.18)"
                  },
                  ticks: {
                    color: "#9db0d4",
                    callback(value) {
                      return fmtBRL.format(value);
                    }
                  },
                  title: {
                    display: true,
                    text: "Capital acumulado (R$)",
                    color: "#9db0d4"
                  }
                }
              }
            }
          });
        }

        function applyMode(modeId) {
          const modeData = getScaledModeData(getModeData(modeId));
          activeMode = modeId;
          renderedModeData = modeData;

          chart.data.labels = modeData.labels || [];
          chart.data.datasets[0].label = "Capital total (" + activeContracts + " contrato" + (activeContracts > 1 ? "s" : "") + ")";
          chart.data.datasets[0].data = modeData.values || [];
          const pointRadius = getPointRadius(modeData.values);
          chart.data.datasets[0].pointRadius = pointRadius;
          chart.data.datasets[0].pointHoverRadius = pointRadius > 0 ? 7 : 4;
          chart.data.datasets[0].pointHitRadius = pointRadius > 0 ? 12 : 6;
          chart.options.scales.y.min = modeData.y_min ?? payload.y_min;
          chart.options.scales.y.max = modeData.y_max ?? payload.y_max;
          chart.update();
          if (summaryChart) {
            summaryChart.data.labels = modeData.labels || [];
            summaryChart.data.datasets[0].label = "Capital total (" + activeContracts + " contrato" + (activeContracts > 1 ? "s" : "") + ")";
            summaryChart.data.datasets[0].data = modeData.values || [];
            summaryChart.data.datasets[0].pointRadius = pointRadius;
            summaryChart.data.datasets[0].pointHoverRadius = pointRadius > 0 ? 6 : 4;
            summaryChart.data.datasets[0].pointHitRadius = pointRadius > 0 ? 10 : 6;
            summaryChart.options.scales.y.min = modeData.y_min ?? payload.y_min;
            summaryChart.options.scales.y.max = modeData.y_max ?? payload.y_max;
            summaryChart.update();
          }

          if (windowRangeText) {
            const startDate = modeData.window_start_date || payload.window_start_date || payload.start_date || "";
            const endDate = modeData.window_end_date || payload.window_end_date || payload.end_date || "";
            windowRangeText.textContent = startDate + " até " + endDate;
          }
          if (chartHintText) {
            chartHintText.textContent = getHintText(modeData);
          }
          refreshContractIndicators();

          modeButtons.forEach((btn) => {
            btn.classList.toggle("is-active", (btn.getAttribute("data-chart-mode") || "") === modeId);
          });
        }

        modeButtons.forEach((btn) => {
          btn.addEventListener("click", () => {
            const modeId = (btn.getAttribute("data-chart-mode") || "").trim();
            if (!modeId) return;
            applyMode(modeId);
          });
        });

        if (contractInput) {
          contractInput.addEventListener("change", () => {
            const nextContracts = normalizeContract(contractInput.value);
            if (nextContracts === activeContracts) {
              contractInput.value = String(activeContracts);
              return;
            }
            activeContracts = nextContracts;
            applyMode(activeMode);
          });

          contractInput.addEventListener("blur", () => {
            contractInput.value = String(normalizeContract(contractInput.value));
          });
        }

        refreshContractIndicators();
        applyMode(activeMode);
      })();
