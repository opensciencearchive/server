import type { FilterPanelData } from "../../lib/types";
import { mountWidget } from "../mount";
import { FilterPanel } from "./FilterPanel";

mountWidget<FilterPanelData>((data, host) => <FilterPanel data={data} host={host} />);
