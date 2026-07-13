import type { ChartData } from "../../lib/types";
import { mountWidget } from "../mount";
import { ChartView } from "./ChartView";

mountWidget<ChartData>("chart", (data) => <ChartView data={data} />);
