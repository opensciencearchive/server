import type { FilterPanelData } from "../../lib/types";
import { mountWidget } from "../mount";
import { FilterPanel } from "./FilterPanel";

mountWidget<FilterPanelData>("filter-panel", (data, host) => <FilterPanel data={data} host={host} />);
