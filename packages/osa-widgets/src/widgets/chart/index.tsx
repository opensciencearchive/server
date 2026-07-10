import type { ChartData } from "../../lib/types";
import { mountWidget } from "../mount";
import { ChartView } from "./ChartView";

mountWidget<ChartData>((data) => <ChartView data={data} />);
