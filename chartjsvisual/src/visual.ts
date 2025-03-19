import Chart from 'chart.js/auto';
import powerbi from "powerbi-visuals-api";

// Importando os tipos necessários do Power BI Visuals API
import IVisualHost = powerbi.extensibility.IVisualHost;
import IVisual = powerbi.extensibility.IVisual;
import VisualUpdateOptions = powerbi.extensibility.visual.VisualUpdateOptions;

export default class Visual implements IVisual {
    private target: HTMLElement;
    private chart: Chart | null = null;

    constructor(options: any) {
        this.target = options.element;
        this.initChart();
    }

    private initChart(): void {
        this.target.innerHTML = "<canvas id='chartCanvas'></canvas>";

        const ctx = document.getElementById('chartCanvas') as HTMLCanvasElement;
        this.chart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio'],
                datasets: [{
                    label: 'Vendas Mensais',
                    data: [120, 190, 300, 500, 700],
                    backgroundColor: ['#36A2EB', '#FF6384', '#FFCE56', '#4BC0C0', '#9966FF'],
                    borderColor: '#333',
                    borderWidth: 2
                }]
            },
            options: {
                responsive: true
            }
        });
    }
}
